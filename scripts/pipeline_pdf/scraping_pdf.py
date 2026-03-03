import argparse
import csv
import json
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")
ARK_PATTERN = re.compile(r"(ark:/12148/[a-z0-9]+)", re.IGNORECASE)


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


class CircuitBreaker429:
    def __init__(self, threshold: int, sleep_seconds: int) -> None:
        self.threshold = max(1, threshold)
        self.sleep_seconds = max(1, sleep_seconds)
        self.consecutive_429 = 0

    @staticmethod
    def is_429_exception(exc: Exception) -> bool:
        if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
            return exc.response.status_code == 429
        if isinstance(exc, requests.exceptions.RetryError):
            return "429" in str(exc)
        return False

    def record_success(self) -> None:
        if self.consecutive_429 > 0:
            print(
                f"[INFO][circuit_breaker] reset consecutive_429={self.consecutive_429} after success"
            )
        self.consecutive_429 = 0

    def record_failure(self, exc: Exception, context: str) -> None:
        if not self.is_429_exception(exc):
            self.consecutive_429 = 0
            return
        self.consecutive_429 += 1
        print(
            f"[WARN][circuit_breaker] 429 streak={self.consecutive_429}/{self.threshold} "
            f"context={context}"
        )
        if self.consecutive_429 >= self.threshold:
            print(
                f"[WARN][circuit_breaker] Sleeping {self.sleep_seconds}s after {self.consecutive_429} consecutive 429"
            )
            time.sleep(self.sleep_seconds)
            self.consecutive_429 = 0


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


def normalize_issue_ark(value: str) -> str:
    match = ARK_PATTERN.search(value)
    if not match:
        raise ValueError(f"ARK introuvable pour le numero: {value}")
    return match.group(1)


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
        status_code = exc.response.status_code
        return str(status_code) if status_code is not None else "http_error"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.RetryError):
        return "retry_error"
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
        "pdf_url",
        "pdf_path",
        "pdf_size_bytes",
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


def cleanup_empty_dirs(start_dir: Path, stop_dir: Path) -> None:
    current = start_dir
    try:
        stop_resolved = stop_dir.resolve()
    except Exception:
        stop_resolved = stop_dir
    while True:
        if not current.exists():
            break
        try:
            current_resolved = current.resolve()
        except Exception:
            current_resolved = current
        if current_resolved == stop_resolved:
            break
        try:
            current.rmdir()
        except OSError:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent


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
        circuit_breaker.record_failure(exc, context=f"url={url}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline PDF - Etape 2: telecharge les PDF a partir des ARK de numeros."
    )
    parser.add_argument("--input", default="input/arks_numeros.json")
    parser.add_argument("--output", default="input/arks_numeros.json")
    parser.add_argument("--output-csv", default="input/tableau_arks_numeros.csv")
    parser.add_argument("--pdf-root", default="pdf_process")
    parser.add_argument("--requests-per-minute", type=int, default=1)
    parser.add_argument("--cb-threshold", type=int, default=5)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument(
        "--user-agent",
        default="memoire-gallica-scraper/1.0 (+contact-local)",
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_csv_path = Path(args.output_csv)
    pdf_root = Path(args.pdf_root)

    payload = load_payload(input_path)
    items = ensure_items(payload)
    session = build_session(args.user_agent)
    limiter = RateLimiter(args.requests_per_minute)
    circuit_breaker = CircuitBreaker429(args.cb_threshold, args.cb_sleep_seconds)

    downloaded_count = 0
    existing_count = 0
    error_count = 0
    skipped_prior_error = 0

    for item in items:
        if str(item.get("status", "")).strip() == "error" and not str(
            item.get("issue_ark", "")
        ).strip():
            print(
                f"[ERROR][pdf][skip_prior_error][{item.get('revue','')}][{item.get('numero_id','')}] "
                f"{item.get('error_stage','')} {item.get('error_code','')} {item.get('error_message','')}"
            )
            skipped_prior_error += 1
            continue

        numero_id_raw = str(item.get("numero_id", "")).strip()
        issue_ark_raw = str(item.get("issue_ark", "")).strip()
        numero_id = sanitize_path_part(numero_id_raw, "numero")

        if not numero_id_raw or not issue_ark_raw:
            item["status"] = "error"
            item["error_stage"] = "pdf_input_validation"
            item["error_code"] = "missing_required_fields"
            item["error_message"] = "numero_id ou issue_ark manquant"
            print(
                f"[ERROR][pdf][pdf_input_validation][{item.get('revue','')}][{numero_id_raw}] "
                f"{item['error_message']}"
            )
            error_count += 1
            continue

        try:
            issue_ark = normalize_issue_ark(issue_ark_raw)
        except Exception as exc:
            item["status"] = "error"
            item["error_stage"] = "pdf_ark_normalization"
            item["error_code"] = error_code_from_exception(exc)
            item["error_message"] = str(exc)
            print(
                f"[ERROR][pdf][pdf_ark_normalization][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']}"
            )
            error_count += 1
            continue

        pdf_url = f"https://gallica.bnf.fr/{issue_ark}.pdf"
        revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
        output_dir = pdf_root / revue / numero_id
        pdf_path = output_dir / f"{numero_id}.pdf"

        item["pdf_url"] = pdf_url
        item["pdf_path"] = pdf_path.as_posix()

        if pdf_path.exists() and pdf_path.stat().st_size > 0 and not args.force:
            item["pdf_size_bytes"] = pdf_path.stat().st_size
            item["status"] = "ok"
            item["error_stage"] = ""
            item["error_code"] = ""
            item["error_message"] = ""
            existing_count += 1
            continue

        try:
            content = download_binary(
                session=session,
                limiter=limiter,
                circuit_breaker=circuit_breaker,
                url=pdf_url,
                timeout_seconds=args.timeout_seconds,
            )
            if not content:
                raise ValueError("Contenu PDF vide")
            output_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = output_dir / f".{numero_id}.pdf.part"
            tmp_path.write_bytes(content)
            tmp_path.replace(pdf_path)
            item["pdf_size_bytes"] = pdf_path.stat().st_size
            item["status"] = "ok"
            item["error_stage"] = ""
            item["error_code"] = ""
            item["error_message"] = ""
            downloaded_count += 1
        except Exception as exc:
            tmp_path = output_dir / f".{numero_id}.pdf.part"
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            cleanup_empty_dirs(output_dir, pdf_root)
            item["status"] = "error"
            item["error_stage"] = "pdf_download"
            item["error_code"] = error_code_from_exception(exc)
            item["error_message"] = str(exc)
            print(
                f"[ERROR][pdf][pdf_download][{item.get('revue','')}][{numero_id}] "
                f"{item['error_code']} {item['error_message']}"
            )
            error_count += 1

    payload["total_issues"] = sum(
        1 for it in items if str(it.get("issue_ark", "")).strip()
    )
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)
    payload["pdf_collection"] = {
        "requests_per_minute": args.requests_per_minute,
        "pdf_root": pdf_root.as_posix(),
        "downloaded": downloaded_count,
        "existing": existing_count,
        "errors": error_count,
        "skipped_prior_error": skipped_prior_error,
    }

    save_json(output_path, payload)
    save_csv(output_csv_path, items)
    print(
        f"Termine: {downloaded_count} PDF telecharges, {existing_count} deja presents, "
        f"{error_count} erreurs, {skipped_prior_error} ignores"
    )
    print(f"JSON mis a jour: {output_path}")
    print(f"CSV mis a jour: {output_csv_path}")
    print(f"PDF root: {pdf_root}")


if __name__ == "__main__":
    main()
