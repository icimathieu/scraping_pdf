
import argparse
import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ISSUES_URL = "https://gallica.bnf.fr/services/Issues"
ARK_PATTERN = re.compile(r"(ark:/12148/[a-z0-9]+)", re.IGNORECASE)
DATE_PATTERN = re.compile(r"(\d{4})/(\d{2})/(\d{2})")


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


def normalize_revue_ark(value: str) -> str:
    match = ARK_PATTERN.search(value)
    if not match:
        raise ValueError(f"ARK introuvable dans la valeur: {value}")
    ark = match.group(1)
    return ark if ark.endswith("/date") else f"{ark}/date"


def normalize_issue_ark(value: str) -> str:
    if value.startswith("ark:/12148/"):
        return value
    return f"ark:/12148/{value}"


def fetch_issues_xml(
    session: requests.Session,
    limiter: RateLimiter,
    circuit_breaker: CircuitBreaker429,
    ark: str,
    year: int | None = None,
    timeout_seconds: int = 30,
) -> ET.Element:
    limiter.wait_turn()
    params = {"ark": ark}
    if year is not None:
        params["date"] = str(year)
    context = f"ark={ark} year={year if year is not None else 'all'}"
    try:
        response = session.get(ISSUES_URL, params=params, timeout=timeout_seconds)
        response.raise_for_status()
        circuit_breaker.record_success()
        return ET.fromstring(response.text)
    except Exception as exc:
        circuit_breaker.record_failure(exc, context=context)
        raise


def parse_years(root: ET.Element) -> List[int]:
    years = []
    for node in root.findall(".//year"):
        text = (node.text or "").strip()
        if text.isdigit():
            years.append(int(text))
    return sorted(set(years))


def parse_issues(root: ET.Element) -> Iterable[Tuple[str, int | None, str]]:
    for node in root.findall(".//issue"):
        ark_raw = node.attrib.get("ark", "").strip()
        if not ark_raw:
            continue
        day_raw = node.attrib.get("dayOfYear", "").strip()
        day_of_year = int(day_raw) if day_raw.isdigit() else None
        precision = (node.text or "").strip()
        yield normalize_issue_ark(ark_raw), day_of_year, precision


def build_numero_id(
    revue_name: str,
    precision: str,
    year: int,
    day_of_year: int | None,
    issue_ark: str,
) -> str:
    match = DATE_PATTERN.search(precision)
    if match:
        y, m, d = match.groups()
        return f"{revue_name}{y}{m}{d}"
    day_part = f"{day_of_year:03d}" if day_of_year is not None else "000"
    return f"{revue_name}{year}{day_part}_{issue_ark.split('/')[-1]}"


def load_revues(path: Path) -> Dict[str, str]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and getattr(exc, "response", None) is not None:
        status_code = exc.response.status_code
        return str(status_code) if status_code is not None else "http_error"
    if isinstance(exc, requests.Timeout):
        return "timeout"
    if isinstance(exc, ET.ParseError):
        return "parse_error"
    return exc.__class__.__name__


def build_error_row(
    revue: str,
    parent_ark_date: str,
    stage: str,
    exc: Exception,
    year: int | None = None,
) -> dict:
    return {
        "revue": revue,
        "parent_ark_date": parent_ark_date,
        "year": year if year is not None else "",
        "day_of_year": "",
        "numero_id": "",
        "issue_ark": "",
        "precision": "",
        "status": "error",
        "pipeline_status": "error",
        "error_stage": stage,
        "error_code": error_code_from_exception(exc),
        "error_message": str(exc),
    }


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def save_csv(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "revue",
        "parent_ark_date",
        "year",
        "day_of_year",
        "numero_id",
        "issue_ark",
        "precision",
        "status",
        "pipeline_status",
        "error_stage",
        "error_code",
        "error_message",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Récupère les ARK des fascicules Gallica via le service Issues."
    )
    parser.add_argument(
        "--input",
        default="input/arks_revues.json",
        help="Fichier JSON des revues {nom_revue: ark_ou_url}.",
    )
    parser.add_argument(
        "--output-json",
        default="input/arks_numeros.json",
        help="Sortie JSON avec la liste des numéros.",
    )
    parser.add_argument(
        "--output-csv",
        default="input/tableau_arks_numeros.csv",
        help="Sortie CSV détaillée.",
    )
    parser.add_argument("--start-year", type=int, default=1870)
    parser.add_argument("--end-year", type=int, default=1914)
    parser.add_argument("--requests-per-minute", type=int, default=10)
    parser.add_argument(
        "--user-agent",
        default="memoire-gallica-scraper/1.0 (+contact-local)",
        help="User-Agent envoyé à Gallica.",
    )
    parser.add_argument("--cb-threshold", type=int, default=5)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_json = Path(args.output_json)
    output_csv = Path(args.output_csv)

    revues = load_revues(input_path)
    session = build_session(args.user_agent)
    limiter = RateLimiter(args.requests_per_minute)
    circuit_breaker = CircuitBreaker429(args.cb_threshold, args.cb_sleep_seconds)

    rows: List[dict] = []

    for revue_name, revue_ark_or_url in revues.items():
        parent_ark_date = ""
        try:
            parent_ark_date = normalize_revue_ark(revue_ark_or_url)
        except Exception as exc:
            print(f"[ERROR][step1][normalize_revue_ark][{revue_name}] {exc}")
            rows.append(
                build_error_row(
                    revue=revue_name,
                    parent_ark_date="",
                    stage="normalize_revue_ark",
                    exc=exc,
                )
            )
            continue

        print(f"[revue] {revue_name} -> {parent_ark_date}")
        try:
            years_root = fetch_issues_xml(session, limiter, circuit_breaker, parent_ark_date)
            years = [
                y
                for y in parse_years(years_root)
                if args.start_year <= y <= args.end_year
            ]
            print(
                f"  {len(years)} années retenues entre {args.start_year} et {args.end_year}"
            )
        except Exception as exc:
            print(f"[ERROR][step1][years][{revue_name}] {exc}")
            rows.append(
                build_error_row(
                    revue=revue_name,
                    parent_ark_date=parent_ark_date,
                    stage="years",
                    exc=exc,
                )
            )
            continue

        for year in years:
            try:
                issues_root = fetch_issues_xml(
                    session,
                    limiter,
                    circuit_breaker,
                    parent_ark_date,
                    year=year,
                )
            except Exception as exc:
                print(f"[ERROR][step1][issues_by_year][{revue_name}][{year}] {exc}")
                rows.append(
                    build_error_row(
                        revue=revue_name,
                        parent_ark_date=parent_ark_date,
                        stage="issues_by_year",
                        exc=exc,
                        year=year,
                    )
                )
                continue

            for issue_ark, day_of_year, precision in parse_issues(issues_root):
                numero_id = build_numero_id(
                    revue_name, precision, year, day_of_year, issue_ark
                )
                row = {
                    "revue": revue_name,
                    "parent_ark_date": parent_ark_date,
                    "year": year,
                    "day_of_year": day_of_year if day_of_year is not None else "",
                    "numero_id": numero_id,
                    "issue_ark": issue_ark,
                    "precision": precision,
                    "status": "ok",
                    "pipeline_status": "",
                    "error_stage": "",
                    "error_code": "",
                    "error_message": "",
                }
                rows.append(row)

    ok_count = sum(1 for row in rows if row.get("status") == "ok")
    error_count = sum(1 for row in rows if row.get("status") == "error")

    payload = {
        "period": {"start_year": args.start_year, "end_year": args.end_year},
        "total_issues": ok_count,
        "total_errors": error_count,
        "total_events": len(rows),
        "items": rows,
    }

    save_json(output_json, payload)
    save_csv(output_csv, rows)
    print(f"Terminé: {ok_count} fascicules exportés, {error_count} erreurs")
    print(f"JSON: {output_json}")
    print(f"CSV : {output_csv}")


if __name__ == "__main__":
    main()
