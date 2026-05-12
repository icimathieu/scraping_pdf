"""Recupere les notices Dublin Core des revues via l'API OAIRecord de Gallica.

Pour chaque revue listee dans input/arks_revues.json, requete
https://gallica.bnf.fr/services/OAIRecord?ark=<ark> et extrait les metadonnees
bibliographiques importantes vers un CSV exploitable dans le memoire.

Usage:
    .venv/bin/python -u scripts/scraping_notices_revues.py
"""

import argparse
import csv
import json
import re
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
NS = {"dc": DC_NS, "oai_dc": OAI_DC_NS}

ARK_PATTERN = re.compile(r"ark:/12148/[A-Za-z0-9._-]+")


class RateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self.requests_per_minute = max(1, requests_per_minute)
        self.window_seconds = 60.0
        self.timestamps: "deque[float]" = deque()

    def wait_turn(self) -> None:
        now = time.monotonic()
        while self.timestamps and (now - self.timestamps[0]) > self.window_seconds:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.requests_per_minute:
            sleep_for = self.window_seconds - (now - self.timestamps[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
        self.timestamps.append(time.monotonic())


def normalize_ark(value: str) -> str:
    """Extrait la forme 'ark:/12148/xxxx' depuis une URL ou une chaine brute.

    Ne garde JAMAIS le suffixe '/date' (specifique a l'API Issues, refuse par OAIRecord).
    """
    match = ARK_PATTERN.search(str(value))
    if not match:
        raise ValueError(f"ARK non reconnu dans la valeur: {value!r}")
    return match.group(0)


def make_session(user_agent: str, timeout: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=2.0,
        respect_retry_after_header=True,
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    session.request_timeout = timeout  # type: ignore[attr-defined]
    return session


def fetch_oai_record(session: requests.Session, ark: str, timeout: int) -> str:
    url = "https://gallica.bnf.fr/services/OAIRecord"
    response = session.get(url, params={"ark": ark}, timeout=timeout)
    response.raise_for_status()
    return response.text


def first_text(element: ET.Element, tag: str, ns: Dict[str, str] = NS) -> str:
    found = element.find(f"dc:{tag}", ns)
    if found is None or found.text is None:
        return ""
    return found.text.strip()


def all_texts(element: ET.Element, tag: str, ns: Dict[str, str] = NS) -> List[str]:
    return [
        (e.text or "").strip()
        for e in element.findall(f"dc:{tag}", ns)
        if (e.text or "").strip()
    ]


def parse_period(date_raw: str) -> Tuple[str, str]:
    """Parse '1884-1954' ou '1884-' ou '1884' en (start, end)."""
    if not date_raw:
        return ("", "")
    parts = re.split(r"\s*-\s*", date_raw, maxsplit=1)
    start = parts[0].strip() if parts else ""
    end = parts[1].strip() if len(parts) > 1 else ""
    return (start, end)


def extract_with_prefix(descriptions: List[str], prefix: str) -> str:
    """Cherche une description commencant par prefix (ex 'Periodicite :').

    Retourne le contenu apres le prefixe, ou '' si non trouve.
    """
    prefix_low = prefix.lower()
    for desc in descriptions:
        if desc.lower().startswith(prefix_low):
            return desc[len(prefix):].lstrip(" :").strip()
    return ""


def extract_issn(identifiers: List[str]) -> str:
    for ident in identifiers:
        m = re.search(r"\bISSN\s*[:\-]?\s*([0-9]{4}-?[0-9X]{3}[0-9X])", ident, re.IGNORECASE)
        if m:
            return m.group(1).replace("-", "")
    return ""


def extract_catalog_url(relations: List[str]) -> str:
    for rel in relations:
        m = re.search(r"https?://catalogue\.bnf\.fr/\S+", rel)
        if m:
            return m.group(0).rstrip(".)")
    return ""


def extract_nb_views(formats: List[str]) -> str:
    for fmt in formats:
        m = re.search(r"Nombre total de vues\s*:\s*(\d+)", fmt)
        if m:
            return m.group(1)
    return ""


def parse_notice_xml(
    xml_text: str,
    start_year: int,
    end_year: int,
) -> Dict[str, Any]:
    root = ET.fromstring(xml_text)

    count_results = root.attrib.get("countResults", "")
    notice = root.find("notice/record/metadata/oai_dc:dc", NS)
    if notice is None:
        return {
            "found": False,
            "count_results": count_results,
            "raw_xml_first_chars": xml_text[:200],
        }

    titles = all_texts(notice, "title")
    descriptions = all_texts(notice, "description")
    publishers = all_texts(notice, "publisher")
    contributors = all_texts(notice, "contributor")
    dates = all_texts(notice, "date")
    identifiers = all_texts(notice, "identifier")
    subjects = all_texts(notice, "subject")
    languages = all_texts(notice, "language")
    relations = all_texts(notice, "relation")
    types = all_texts(notice, "type")
    formats = all_texts(notice, "format")
    sources = all_texts(notice, "source")

    # Champs hors Dublin Core dans <results>
    typedoc = (root.findtext("typedoc") or "").strip()
    dewey = (root.findtext("dewey") or "").strip()
    sdewey = (root.findtext("sdewey") or "").strip()
    first_indexation_date = (root.findtext("first_indexation_date") or "").strip()

    # Annees disponibles (avec nbIssue) en hors Dublin Core
    years_data: List[Tuple[int, int]] = []
    for date_el in root.findall("date"):
        text = (date_el.text or "").strip()
        if text.isdigit():
            year = int(text)
            try:
                nb = int(date_el.attrib.get("nbIssue", "0"))
            except ValueError:
                nb = 0
            years_data.append((year, nb))

    years_available = [y for y, _ in years_data]
    nb_issues_total = sum(nb for _, nb in years_data)
    years_in_period = [y for y, _ in years_data if start_year <= y <= end_year]
    nb_issues_in_period = sum(nb for y, nb in years_data if start_year <= y <= end_year)

    date_publication = " ; ".join(dates)
    date_start, date_end = parse_period(dates[0]) if dates else ("", "")

    periodicity = extract_with_prefix(descriptions, "Periodicite")
    if not periodicity:
        periodicity = extract_with_prefix(descriptions, "Périodicité")
    collection_state = extract_with_prefix(descriptions, "Etat de collection")
    if not collection_state:
        collection_state = extract_with_prefix(descriptions, "État de collection")
    title_variants = extract_with_prefix(descriptions, "Variante(s) de titre")
    if not title_variants:
        title_variants = extract_with_prefix(descriptions, "Variantes de titre")
    note_ensemble = extract_with_prefix(descriptions, "Appartient à l’ensemble documentaire")
    if not note_ensemble:
        note_ensemble = extract_with_prefix(descriptions, "Appartient a l'ensemble documentaire")

    issn = extract_issn(identifiers)
    catalog_url = extract_catalog_url(relations)
    nb_views = extract_nb_views(formats)

    return {
        "found": True,
        "count_results": count_results,
        "title": titles[0] if titles else "",
        "title_variants": title_variants,
        "publishers": " | ".join(publishers),
        "contributors": " | ".join(contributors),
        "date_publication": date_publication,
        "date_start": date_start,
        "date_end": date_end,
        "periodicity": periodicity,
        "collection_state": collection_state,
        "issn": issn,
        "language": " | ".join(languages),
        "subjects": " | ".join(subjects),
        "types": " | ".join(types),
        "dewey": dewey,
        "sdewey": sdewey,
        "typedoc": typedoc,
        "source": " | ".join(sources),
        "ensemble_documentaire": note_ensemble,
        "catalog_url": catalog_url,
        "nb_total_views": nb_views,
        "first_indexation_date": first_indexation_date,
        "nb_years_available": str(len(years_available)),
        "years_available": "|".join(str(y) for y in years_available),
        "nb_issues_total": str(nb_issues_total),
        "nb_years_in_period": str(len(years_in_period)),
        "years_in_period": "|".join(str(y) for y in years_in_period),
        "nb_issues_in_period": str(nb_issues_in_period),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recupere les notices OAI Dublin Core des revues Gallica vers CSV."
    )
    parser.add_argument("--input", default="input/arks_revues.json")
    parser.add_argument("--output", default="input/notices_revues.csv")
    parser.add_argument("--requests-per-minute", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--start-year", type=int, default=1870)
    parser.add_argument("--end-year", type=int, default=1914)
    parser.add_argument(
        "--user-agent",
        default="memoire-gallica-scraper/1.0 (+contact-local)",
    )
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if not input_path.exists():
        print(f"[ERROR] Fichier d'entree introuvable: {input_path}", file=sys.stderr)
        return 1

    with input_path.open("r", encoding="utf-8") as fh:
        revues = json.load(fh)
    if not isinstance(revues, dict):
        print("[ERROR] arks_revues.json doit etre un objet {nom: ark/url}.", file=sys.stderr)
        return 1

    print(f"[INFO] {len(revues)} revues a traiter (rpm={args.requests_per_minute})")

    session = make_session(args.user_agent, args.timeout_seconds)
    limiter = RateLimiter(args.requests_per_minute)

    fieldnames = [
        "nom_court",
        "ark",
        "ark_url",
        "title",
        "title_variants",
        "publishers",
        "contributors",
        "date_publication",
        "date_start",
        "date_end",
        "periodicity",
        "collection_state",
        "issn",
        "language",
        "subjects",
        "types",
        "typedoc",
        "dewey",
        "sdewey",
        "source",
        "ensemble_documentaire",
        "catalog_url",
        "nb_total_views",
        "first_indexation_date",
        "nb_years_available",
        "years_available",
        "nb_issues_total",
        "nb_years_in_period",
        "years_in_period",
        "nb_issues_in_period",
        "fetch_status",
        "fetch_error",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()

        for idx, (nom_court, raw_ark) in enumerate(revues.items(), start=1):
            print(f"[{idx}/{len(revues)}] {nom_court}")
            row: Dict[str, str] = {f: "" for f in fieldnames}
            row["nom_court"] = nom_court

            try:
                ark = normalize_ark(raw_ark)
            except ValueError as exc:
                row["fetch_status"] = "error"
                row["fetch_error"] = f"ark_parse: {exc}"
                writer.writerow(row)
                fh.flush()
                continue

            row["ark"] = ark
            row["ark_url"] = f"https://gallica.bnf.fr/{ark}"

            limiter.wait_turn()
            try:
                xml_text = fetch_oai_record(session, ark, args.timeout_seconds)
            except requests.HTTPError as exc:
                code = (
                    exc.response.status_code
                    if exc.response is not None
                    else "unknown"
                )
                row["fetch_status"] = "error"
                row["fetch_error"] = f"http_{code}: {exc}"
                writer.writerow(row)
                fh.flush()
                continue
            except requests.RequestException as exc:
                row["fetch_status"] = "error"
                row["fetch_error"] = f"network: {exc}"
                writer.writerow(row)
                fh.flush()
                continue

            try:
                parsed = parse_notice_xml(xml_text, args.start_year, args.end_year)
            except ET.ParseError as exc:
                row["fetch_status"] = "error"
                row["fetch_error"] = f"xml_parse: {exc}"
                writer.writerow(row)
                fh.flush()
                continue

            if not parsed.get("found"):
                row["fetch_status"] = "not_found"
                row["fetch_error"] = (
                    f"countResults={parsed.get('count_results','?')}"
                )
                writer.writerow(row)
                fh.flush()
                continue

            for key, value in parsed.items():
                if key == "found":
                    continue
                if key == "count_results":
                    continue
                if key in fieldnames:
                    row[key] = value if isinstance(value, str) else str(value)
            row["fetch_status"] = "ok"
            writer.writerow(row)
            fh.flush()

    print(f"[OK] CSV ecrit: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
