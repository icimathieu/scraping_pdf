import argparse
import csv
import json
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")


class RateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self.requests_per_minute = max(1, requests_per_minute)
        self.window_seconds = 60.0
        self.timestamps = deque()

    def wait_turn(self) -> None:
        now = time.monotonic()
        while self.timestamps and (now - self.timestamps[0]) > self.window_seconds:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.requests_per_minute:
            sleep_for = self.window_seconds - (now - self.timestamps[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            while self.timestamps and (now - self.timestamps[0]) > self.window_seconds:
                self.timestamps.popleft()
        self.timestamps.append(time.monotonic())


def build_session(user_agent: str) -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def sanitize_path_part(value: str, fallback: str) -> str:
    clean = NON_ALNUM_PATTERN.sub("_", value).strip("._")
    return clean if clean else fallback


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
        status_code = exc.response.status_code
        return str(status_code) if status_code is not None else "http_error"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, json.JSONDecodeError):
        return "json_decode_error"
    if isinstance(exc, ValueError):
        return "value_error"
    return exc.__class__.__name__


def load_payload(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Le fichier d'entree doit contenir un objet JSON.")
    return payload


def ensure_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = payload.get("items")
    if isinstance(items, list):
        return items
    raise ValueError("Le fichier d'entree doit contenir une liste 'items'.")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def save_csv(path: Path, items: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "revue",
        "parent_ark_date",
        "year",
        "day_of_year",
        "numero_id",
        "issue_ark",
        "precision",
        "manifest_url",
        "manifest_path",
        "image_output_dir",
        "images_total",
        "images_downloaded",
        "images_existing",
        "images_errors",
        "status",
        "error_stage",
        "error_code",
        "error_message",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow({key: item.get(key, "") for key in fieldnames})


def resolve_manifest_path(
    item: Dict[str, Any],
    manifest_root: Path,
) -> Path:
    manifest_path_raw = str(item.get("manifest_path", "")).strip()
    if manifest_path_raw:
        return Path(manifest_path_raw)

    revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
    numero_id = sanitize_path_part(str(item.get("numero_id", "")), "numero")
    return manifest_root / revue / f"{numero_id}.manifest.json"


def extract_canvas_image_service_id(canvas: Dict[str, Any]) -> str:
    images = canvas.get("images") or []
    if not images:
        raise ValueError("Canvas sans images")
    image_annotation = images[0] or {}
    resource = image_annotation.get("resource") or {}
    service = resource.get("service") or {}
    if isinstance(service, list):
        service = service[0] if service else {}
    if isinstance(service, dict):
        service_id = service.get("@id") or service.get("id")
        if service_id:
            return str(service_id)

    resource_id = resource.get("@id") or resource.get("id")
    if isinstance(resource_id, str) and "/full/" in resource_id:
        return resource_id.split("/full/")[0]
    raise ValueError("URL IIIF introuvable pour le canvas")


def build_image_url(service_id: str, quality: str, image_format: str) -> str:
    return f"{service_id}/full/full/0/{quality}.{image_format}"


def download_binary(session: requests.Session, limiter: RateLimiter, url: str, timeout_seconds: int) -> bytes:
    limiter.wait_turn()
    response = session.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Etape 3: telecharge les images a partir des manifests IIIF."
    )
    parser.add_argument(
        "--input",
        default="input/arks_numeros.json",
        help="JSON d'entree contenant items[].",
    )
    parser.add_argument(
        "--output",
        default="input/arks_numeros.json",
        help="JSON de sortie enrichi avec les infos images.",
    )
    parser.add_argument(
        "--output-csv",
        default="input/tableau_arks_numeros.csv",
        help="CSV enrichi avec les infos images.",
    )
    parser.add_argument(
        "--manifest-root",
        default="data_process",
        help="Racine des manifests si manifest_path n'est pas renseigne.",
    )
    parser.add_argument(
        "--image-root",
        default="images_process",
        help="Racine de sortie des images.",
    )
    parser.add_argument("--requests-per-minute", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--quality", default="bitonal")
    parser.add_argument("--format", default="jpg")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Limite de pages par numero (0 = toutes les pages).",
    )
    parser.add_argument(
        "--user-agent",
        default="memoire-gallica-scraper/1.0 (+contact-local)",
        help="User-Agent envoye a Gallica.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-telecharge les images meme si le fichier existe deja.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_csv_path = Path(args.output_csv)
    manifest_root = Path(args.manifest_root)
    image_root = Path(args.image_root)

    payload = load_payload(input_path)
    items = ensure_items(payload)
    session = build_session(args.user_agent)
    limiter = RateLimiter(args.requests_per_minute)

    processed_ok = 0
    processed_error = 0
    skipped_prior_error = 0

    for item in items:
        item.pop("images_error", None)

        # Conserver les erreurs structurantes des etapes precedentes.
        if str(item.get("status", "")).strip() == "error" and not str(
            item.get("issue_ark", "")
        ).strip():
            print(
                f"[ERROR][step3][skip_prior_error][{item.get('revue','')}][{item.get('numero_id','')}] "
                f"{item.get('error_stage','')} {item.get('error_code','')} {item.get('error_message','')}"
            )
            skipped_prior_error += 1
            continue

        revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
        numero_id_raw = str(item.get("numero_id", "")).strip()
        numero_id = sanitize_path_part(numero_id_raw, "numero")

        if not numero_id_raw:
            item["status"] = "error"
            item["error_stage"] = "images_input_validation"
            item["error_code"] = "missing_numero_id"
            item["error_message"] = "numero_id manquant"
            print(
                f"[ERROR][step3][images_input_validation][{item.get('revue','')}][{numero_id_raw}] "
                f"{item['error_message']}"
            )
            processed_error += 1
            continue

        output_dir = image_root / revue / numero_id
        output_dir.mkdir(parents=True, exist_ok=True)
        item["image_output_dir"] = output_dir.as_posix()

        manifest_path = resolve_manifest_path(item, manifest_root)
        item["manifest_path"] = manifest_path.as_posix()
        if not manifest_path.exists():
            item["status"] = "error"
            item["error_stage"] = "images_manifest_path"
            item["error_code"] = "manifest_not_found"
            item["error_message"] = f"Manifest introuvable: {manifest_path}"
            print(
                f"[ERROR][step3][images_manifest_path][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']}"
            )
            item["images_total"] = 0
            item["images_downloaded"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            processed_error += 1
            continue

        try:
            with manifest_path.open("r", encoding="utf-8") as fh:
                manifest = json.load(fh)
        except Exception as exc:
            item["status"] = "error"
            item["error_stage"] = "images_manifest_load"
            item["error_code"] = error_code_from_exception(exc)
            item["error_message"] = str(exc)
            print(
                f"[ERROR][step3][images_manifest_load][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']}"
            )
            item["images_total"] = 0
            item["images_downloaded"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            processed_error += 1
            continue

        sequences = manifest.get("sequences") or []
        if not sequences or not isinstance(sequences, list):
            item["status"] = "error"
            item["error_stage"] = "images_manifest_parse"
            item["error_code"] = "missing_sequences"
            item["error_message"] = "Manifest sans sequences."
            print(
                f"[ERROR][step3][images_manifest_parse][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']}"
            )
            item["images_total"] = 0
            item["images_downloaded"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            processed_error += 1
            continue

        canvases = (sequences[0] or {}).get("canvases") or []
        if args.max_pages and args.max_pages > 0:
            canvases = canvases[: args.max_pages]

        total_pages = len(canvases)
        downloaded = 0
        existing = 0
        errors = 0
        first_error: Tuple[str, str] | None = None

        for page_index, canvas in enumerate(canvases, start=1):
            image_file = output_dir / f"page_{page_index:04d}.{args.format}"
            if image_file.exists() and image_file.stat().st_size > 0 and not args.force:
                existing += 1
                continue

            try:
                service_id = extract_canvas_image_service_id(canvas)
                image_url = build_image_url(service_id, args.quality, args.format)
                content = download_binary(session, limiter, image_url, args.timeout_seconds)
                if not content:
                    raise ValueError("Contenu image vide")
                image_file.write_bytes(content)
                downloaded += 1
            except Exception as exc:
                errors += 1
                if first_error is None:
                    first_error = (error_code_from_exception(exc), str(exc))

        item["images_total"] = total_pages
        item["images_downloaded"] = downloaded
        item["images_existing"] = existing
        item["images_errors"] = errors

        if errors > 0:
            item["status"] = "error"
            item["error_stage"] = "image_download"
            item["error_code"] = first_error[0] if first_error else "unknown_error"
            item["error_message"] = first_error[1] if first_error else "Erreur image non detaillee"
            print(
                f"[ERROR][step3][image_download][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']} (errors={errors}/{total_pages})"
            )
            processed_error += 1
        else:
            item["status"] = "ok"
            item["error_stage"] = ""
            item["error_code"] = ""
            item["error_message"] = ""
            processed_ok += 1

    payload["total_issues"] = sum(
        1 for it in items if str(it.get("issue_ark", "")).strip()
    )
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)
    payload["images_collection"] = {
        "requests_per_minute": args.requests_per_minute,
        "manifest_root": manifest_root.as_posix(),
        "image_root": image_root.as_posix(),
        "processed_ok": processed_ok,
        "processed_error": processed_error,
        "skipped_prior_error": skipped_prior_error,
    }

    save_json(output_path, payload)
    save_csv(output_csv_path, items)
    print(
        "Termine: "
        f"{processed_ok} numeros OK, {processed_error} numeros en erreur, "
        f"{skipped_prior_error} ignores (erreurs precedentes)"
    )
    print(f"JSON mis a jour: {output_path}")
    print(f"CSV mis a jour: {output_csv_path}")
    print(f"Images: {image_root}")


if __name__ == "__main__":
    main()
