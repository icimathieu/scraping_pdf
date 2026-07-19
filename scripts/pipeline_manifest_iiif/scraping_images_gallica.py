import argparse
import csv
import json
import random
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
    """Limiteur en fenetre glissante avec jitter optionnel pour evader la
    detection de pattern regulier cote serveur."""

    def __init__(self, requests_per_minute: int, jitter_seconds: float = 1.5) -> None:
        self.requests_per_minute = max(1, requests_per_minute)
        self.window_seconds = 60.0
        self.jitter_seconds = max(0.0, jitter_seconds)
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
        # Jitter aleatoire pour eviter le pattern regulier exact (15.000s 15.000s...)
        if self.jitter_seconds > 0:
            time.sleep(random.uniform(0, self.jitter_seconds))
        self.timestamps.append(time.monotonic())


class CircuitBreakerStop(RuntimeError):
    pass


class CircuitBreaker429:
    """Compte toutes les erreurs consecutives (pas seulement les 429).
    Apres `threshold` echecs : sleep `sleep_seconds`. Apres `max_cooldowns`
    sleeps : raise CircuitBreakerStop pour arret definitif."""

    def __init__(self, threshold: int, sleep_seconds: int, max_cooldowns: int = 2) -> None:
        self.threshold = max(1, threshold)
        self.sleep_seconds = max(1, sleep_seconds)
        self.max_cooldowns = max(1, max_cooldowns)
        self.consecutive_failures = 0
        self.cooldowns_used = 0

    @staticmethod
    def is_429_exception(exc: Exception) -> bool:
        if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
            return exc.response.status_code == 429
        if isinstance(exc, requests.exceptions.RetryError):
            return "429" in str(exc)
        return False

    @staticmethod
    def is_definitive_skip(exc: Exception) -> bool:
        """404 = la page n'existe pas cote Gallica (trou de pagination). Erreur
        permanente : ne doit PAS compter dans le breaker, qui est reserve aux
        429 et erreurs reseau transitoires. Reessayer ne la fera jamais
        apparaitre, donc une rafale de 404 ne doit pas declencher de cooldown."""
        if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
            return exc.response.status_code == 404
        return False

    def record_skip(self, context: str) -> None:
        """Page absente (404) : evenement definitif, neutre pour le breaker.
        On ne touche pas consecutive_failures (ni +1 ni reset) : seules les
        vraies erreurs transitoires comptent vers le cooldown."""
        print(f"[INFO][circuit_breaker] skip page absente (404) context={context}")

    def record_success(self) -> None:
        if self.consecutive_failures > 0:
            print(
                f"[INFO][circuit_breaker] reset consecutive_failures={self.consecutive_failures} after success"
            )
        self.consecutive_failures = 0

    def record_failure(self, exc: Exception, context: str) -> None:
        self.consecutive_failures += 1
        is_throttle = self.is_429_exception(exc)
        kind = "429" if is_throttle else exc.__class__.__name__
        print(
            f"[WARN][circuit_breaker] failures streak={self.consecutive_failures}/{self.threshold} "
            f"kind={kind} context={context}"
        )
        if self.consecutive_failures >= self.threshold:
            if self.cooldowns_used >= self.max_cooldowns:
                raise CircuitBreakerStop(
                    f"circuit_breaker_stop: {self.consecutive_failures} echecs consecutifs "
                    f"malgre {self.cooldowns_used} cooldown(s); arret definitif."
                )
            print(
                f"[WARN][circuit_breaker] Sleeping {self.sleep_seconds}s after "
                f"{self.consecutive_failures} consecutive failures "
                f"(cooldown {self.cooldowns_used + 1}/{self.max_cooldowns})"
            )
            time.sleep(self.sleep_seconds)
            self.cooldowns_used += 1
            self.consecutive_failures = 0


def build_session(user_agent: str) -> requests.Session:
    # Retry total=2 : visibilite reelle des 429 (avant : total=5 masquait
    # 4 retries silencieux par 429 visible dans nos stats).
    retry = Retry(
        total=2,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({
        "User-Agent": user_agent,
        "Accept": "image/jpeg,image/png,image/*,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        "Referer": "https://gallica.bnf.fr/",
        "DNT": "1",
    })
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
        "pipeline_status",
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


def build_image_url(service_id: str, quality: str, image_format: str, size: str = "full") -> str:
    # size par defaut "!3600,3600" (fixe via --iiif-size) : on demande a Gallica
    # de downscaler cote serveur (cote long <= 3600 px, ~300 DPI). Gains : (1) evite
    # les 403 intermittents que Gallica renvoie sur la pleine resolution ; (2) download
    # ~2x plus leger ; (3) conversion locale allegee (plus de resampling 24 Mpx, juste
    # la mise en gris). Region reste "full" (page entiere).
    return f"{service_id}/full/{size}/0/{quality}.{image_format}"


# Normalisation des images stockees a ~300 DPI gris. Gallica ignore parfois la
# qualite `bitonal` et sert de la couleur pleine, parfois en tres haute
# resolution (~525 DPI / 24 MP) : inutile pour l'OCR (standard = 300 DPI) et
# ingerable (corpus complet ~1,4 To en l'etat contre ~150-250 Go normalise).
# MAX_EDGE borne le cote long ; COMPRESS_LEVEL=9 = quasi-optimal sans le cout
# CPU de optimize=True (qui rendait la conversion 5x plus lente).
IMAGE_MAX_EDGE = 3600
IMAGE_COMPRESS_LEVEL = 9


def downscale_and_grayscale_png(content: bytes) -> bytes:
    """Normalise une image en PNG 8-bit gris, cote long <= IMAGE_MAX_EDGE.

    - Couleur (RGB) -> gris (L) : l'OCR n'utilise pas la couleur.
    - Surdimensionnee (> ~300 DPI) -> redimensionnee (LANCZOS), aspect preserve.
    - Deja en gris (L/1) ET a la bonne taille -> renvoyee intacte (aucun
      re-encodage, donc aucune degradation des pages deja correctes).
    En cas d'echec Pillow, renvoie l'octet brut : on ne perd jamais un
    telechargement reussi."""
    try:
        from io import BytesIO
        from PIL import Image

        with Image.open(BytesIO(content)) as im:
            needs_gray = im.mode not in ("L", "1")
            needs_resize = max(im.size) > IMAGE_MAX_EDGE
            if not needs_gray and not needs_resize:
                return content
            if needs_gray:
                im = im.convert("L")
            if needs_resize:
                scale = IMAGE_MAX_EDGE / max(im.size)
                im = im.resize(
                    (round(im.width * scale), round(im.height * scale)),
                    Image.LANCZOS,
                )
            out = BytesIO()
            im.save(out, format="PNG", compress_level=IMAGE_COMPRESS_LEVEL)
            return out.getvalue()
    except Exception:
        return content


def download_binary(
    session: requests.Session,
    limiter: RateLimiter,
    circuit_breaker: CircuitBreaker429,
    url: str,
    timeout_seconds: int,
) -> bytes:
    limiter.wait_turn()
    try:
        response = session.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        circuit_breaker.record_success()
        return response.content
    except Exception as exc:
        if CircuitBreaker429.is_definitive_skip(exc):
            # 404 : page inexistante cote Gallica, ne compte pas dans le breaker.
            circuit_breaker.record_skip(context=f"url={url}")
        else:
            circuit_breaker.record_failure(exc, context=f"url={url}")
        raise


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
        default="manifest_iiif_process",
        help="Racine des manifests si manifest_path n'est pas renseigne.",
    )
    parser.add_argument(
        "--image-root",
        default="images_process",
        help="Racine de sortie des images.",
    )
    parser.add_argument("--requests-per-minute", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--jitter-seconds", type=float, default=1.5,
        help="Jitter aleatoire ajoute apres chaque tick rate-limit, evite le pattern regulier exact.")
    parser.add_argument("--cb-threshold", type=int, default=5)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--cb-max-cooldowns", type=int, default=2,
        help="Nombre max de pauses du circuit breaker avant arret definitif.")
    parser.add_argument("--quality", default="bitonal")
    parser.add_argument("--format", default="jpg")
    parser.add_argument(
        "--iiif-size",
        default="!3600,3600",
        help="Parametre size IIIF (defaut '!3600,3600' = downscale serveur cote long "
             "<=3600 px). 'full' = pleine resolution (que Gallica 403 par intermittence).",
    )
    parser.add_argument(
        "--no-grayscale",
        dest="grayscale",
        action="store_false",
        help="Desactive la normalisation (gris 8-bit + downscale ~300 DPI) "
             "appliquee par defaut a chaque image (indispensable cote disque).",
    )
    parser.set_defaults(grayscale=True)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Limite de pages par numero (0 = toutes les pages).",
    )
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:150.0) Gecko/20100101 Firefox/150.0",
        help="User-Agent envoye a Gallica. Defaut: Firefox 150 macOS (UA realiste).",
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
    limiter = RateLimiter(args.requests_per_minute, args.jitter_seconds)
    circuit_breaker = CircuitBreaker429(
        args.cb_threshold, args.cb_sleep_seconds, args.cb_max_cooldowns
    )

    processed_ok = 0
    processed_error = 0
    skipped_prior_error = 0
    stopped_by_cb_message = ""

    for item in items:
        if stopped_by_cb_message:
            break
        item.pop("images_error", None)

        # Conserver les erreurs structurantes des etapes precedentes.
        if str(item.get("status", "")).strip() == "error" and not str(
            item.get("issue_ark", "")
        ).strip():
            item["pipeline_status"] = "error"
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
            item["pipeline_status"] = "error"
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
            item["pipeline_status"] = "error"
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
            item["pipeline_status"] = "error"
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
            item["pipeline_status"] = "error"
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
                image_url = build_image_url(service_id, args.quality, args.format, args.iiif_size)
                content = download_binary(
                    session,
                    limiter,
                    circuit_breaker,
                    image_url,
                    args.timeout_seconds,
                )
                if not content:
                    raise ValueError("Contenu image vide")
                if args.grayscale:
                    content = downscale_and_grayscale_png(content)
                image_file.write_bytes(content)
                downloaded += 1
            except CircuitBreakerStop as cb_exc:
                stopped_by_cb_message = str(cb_exc)
                print(f"[ERROR][step3][circuit_breaker_stop] {stopped_by_cb_message}")
                if first_error is None:
                    first_error = ("circuit_breaker_stop", stopped_by_cb_message)
                break
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
            item["pipeline_status"] = "error"
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
            item["pipeline_status"] = "done"
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

    if stopped_by_cb_message:
        import sys
        print(f"[ERROR][step3] Arret par circuit breaker: {stopped_by_cb_message}")
        sys.exit(2)


if __name__ == "__main__":
    main()
