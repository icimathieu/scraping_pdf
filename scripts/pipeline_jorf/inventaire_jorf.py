#!/usr/bin/env python3
"""Inventaire du Journal officiel de la Republique francaise (1870-1880) sur Gallica.

Etape 1 de la pipeline JORF. Produit, pour le titre `cb328020909`, la liste de
tous les fascicules et leur nombre de pages.

Deux phases :

1. **Enumeration** via le service `Issues` (meme API que l'etape 1 des revues) :
   annees disponibles, puis fascicules par annee. ~12 requetes.
2. **Pagination** via le service `Pagination`, qui renvoie `nbVueImages` pour un
   ARK donne.

Pourquoi `Pagination` et pas le manifest IIIF
---------------------------------------------
Le manifest IIIF est le goulot de la voie IIIF : il ne tolere qu'~1 req/min
(cf. CLAUDE.md §8), soit >5 jours pour 7 611 fascicules. Or :

- `Pagination` tient **40 req/min sans un seul 429** (mesure du 2026-09-06) ;
- `nbVueImages` est **exactement** le nombre de canvas du manifest (verifie
  4/4 sur des fascicules de 4, 16, 60 et 80 pages) ;
- l'URL du service image IIIF de Gallica est **deterministe** :
  `https://gallica.bnf.fr/iiif/<ark>/f<n>` pour n = 1..nbVueImages.

On peut donc reconstruire des manifests synthetiques (cf.
`construire_entree_jorf.py`) et economiser ces 5 jours.

La phase 2 est **reprenable** : chaque reponse est ecrite au fil de l'eau dans
un ledger JSONL, relu au demarrage pour ne jamais redemander un ARK connu.
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ISSUES_URL = "https://gallica.bnf.fr/services/Issues"
PAGINATION_URL = "https://gallica.bnf.fr/services/Pagination"

NB_VUES_PATTERN = re.compile(r"<nbVueImages>(\d+)</nbVueImages>")
DATE_PATTERN = re.compile(r"^(\d{1,2})\s+(\S+)\s+(\d{4})$")

MOIS = {
    "janvier": 1, "fevrier": 2, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "aout": 8, "août": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "decembre": 12, "décembre": 12,
}

# Gallica renvoie 403 aux User-Agent non navigateur sur les services /services/*.
UA_DEFAUT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:150.0) "
    "Gecko/20100101 Firefox/150.0"
)


class RateLimiter:
    """Fenetre glissante d'une minute (meme logique que le reste de la pipeline)."""

    def __init__(self, requests_per_minute: int) -> None:
        self.requests_per_minute = max(1, requests_per_minute)
        self.timestamps: deque = deque()

    def wait_turn(self) -> None:
        now = time.monotonic()
        while self.timestamps and (now - self.timestamps[0]) > 60.0:
            self.timestamps.popleft()
        if len(self.timestamps) >= self.requests_per_minute:
            sleep_for = 60.0 - (now - self.timestamps[0]) + 0.05
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.monotonic()
            while self.timestamps and (now - self.timestamps[0]) > 60.0:
                self.timestamps.popleft()
        self.timestamps.append(time.monotonic())


def build_session(user_agent: str) -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parse_date_fr(label: str) -> str | None:
    """'05 septembre 1870' -> '1870-09-05'. None si le libelle n'est pas une date
    de jour (certains fascicules ne portent que l'annee : ce sont typiquement des
    tables, ecartees plus loin)."""
    match = DATE_PATTERN.match(label.strip())
    if not match:
        return None
    jour, mois, annee = match.groups()
    numero_mois = MOIS.get(mois.lower())
    if numero_mois is None:
        return None
    return f"{int(annee):04d}-{numero_mois:02d}-{int(jour):02d}"


def enumerer_fascicules(
    session: requests.Session,
    limiter: RateLimiter,
    ark_titre: str,
    start_year: int,
    end_year: int,
    timeout: int,
) -> List[Dict]:
    limiter.wait_turn()
    reponse = session.get(ISSUES_URL, params={"ark": ark_titre}, timeout=timeout)
    reponse.raise_for_status()
    racine = ET.fromstring(reponse.text)
    annees = sorted(
        {
            int((n.text or "").strip())
            for n in racine.findall(".//year")
            if (n.text or "").strip().isdigit()
        }
    )
    annees = [a for a in annees if start_year <= a <= end_year]
    print(f"[issues] {len(annees)} annees : {annees[0]}-{annees[-1]}", flush=True)

    fascicules: List[Dict] = []
    for annee in annees:
        limiter.wait_turn()
        reponse = session.get(
            ISSUES_URL, params={"ark": ark_titre, "date": str(annee)}, timeout=timeout
        )
        reponse.raise_for_status()
        noeuds = ET.fromstring(reponse.text).findall(".//issue")
        for noeud in noeuds:
            ark = noeud.attrib.get("ark", "").strip()
            if not ark:
                continue
            libelle = (noeud.text or "").strip()
            fascicules.append(
                {
                    "ark": ark,
                    "annee": annee,
                    "day_of_year": noeud.attrib.get("dayOfYear", "").strip(),
                    "libelle": libelle,
                    "date": parse_date_fr(libelle),
                }
            )
        print(f"[issues] {annee} : {len(noeuds)} fascicules", flush=True)
    return fascicules


def charger_ledger(chemin: Path) -> Dict[str, Dict]:
    """Relit le JSONL de la phase 2. Une ligne corrompue (run tue en pleine
    ecriture) est ignoree plutot que de faire echouer la reprise."""
    connus: Dict[str, Dict] = {}
    if not chemin.exists():
        return connus
    with chemin.open("r", encoding="utf-8") as fh:
        for ligne in fh:
            ligne = ligne.strip()
            if not ligne:
                continue
            try:
                enregistrement = json.loads(ligne)
            except json.JSONDecodeError:
                continue
            ark = enregistrement.get("ark")
            # On ne considere comme "connu" qu'un succes : un ARK en erreur sera
            # retente au run suivant.
            if ark and enregistrement.get("pages") is not None:
                connus[ark] = enregistrement
    return connus


def nombre_de_pages(
    session: requests.Session,
    limiter: RateLimiter,
    ark: str,
    timeout: int,
) -> int | None:
    limiter.wait_turn()
    reponse = session.get(PAGINATION_URL, params={"ark": ark}, timeout=timeout)
    reponse.raise_for_status()
    match = NB_VUES_PATTERN.search(reponse.text)
    return int(match.group(1)) if match else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ark", default="ark:/12148/cb328020909/date")
    parser.add_argument("--start-year", type=int, default=1870)
    parser.add_argument("--end-year", type=int, default=1880)
    parser.add_argument("--out-issues", default="input/jorf_fascicules.json")
    parser.add_argument("--out-pages", default="input/jorf_pagination.jsonl")
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=30,
        help="30 par defaut : mesure a 40/min sans 429 le 2026-09-06, on garde "
             "une marge de securite (meme logique que RPM=4 sur l'API Image).",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--user-agent", default=UA_DEFAUT)
    parser.add_argument(
        "--echecs-consecutifs-max",
        type=int,
        default=15,
        help="Arret net si Gallica refuse en rafale (evite de marteler pour rien).",
    )
    args = parser.parse_args()

    chemin_issues = Path(args.out_issues)
    chemin_pages = Path(args.out_pages)
    chemin_issues.parent.mkdir(parents=True, exist_ok=True)
    chemin_pages.parent.mkdir(parents=True, exist_ok=True)

    session = build_session(args.user_agent)
    limiter = RateLimiter(args.requests_per_minute)

    # --- Phase 1 : enumeration (rejouee seulement si le fichier manque) --------
    if chemin_issues.exists():
        fascicules = json.loads(chemin_issues.read_text(encoding="utf-8"))["fascicules"]
        print(f"[issues] {len(fascicules)} fascicules relus depuis {chemin_issues}")
    else:
        fascicules = enumerer_fascicules(
            session, limiter, args.ark, args.start_year, args.end_year,
            args.timeout_seconds,
        )
        chemin_issues.write_text(
            json.dumps(
                {"ark_titre": args.ark, "total": len(fascicules), "fascicules": fascicules},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"[issues] {len(fascicules)} fascicules ecrits dans {chemin_issues}")

    # --- Phase 2 : pagination, reprenable -------------------------------------
    connus = charger_ledger(chemin_pages)
    restants = [f for f in fascicules if f["ark"] not in connus]
    print(
        f"[pagination] {len(connus)} deja connus, {len(restants)} a interroger "
        f"(~{len(restants) / max(1, args.requests_per_minute) / 60:.1f} h a "
        f"{args.requests_per_minute}/min)",
        flush=True,
    )

    echecs_consecutifs = 0
    debut = time.time()
    with chemin_pages.open("a", encoding="utf-8") as ledger:
        for index, fascicule in enumerate(restants, start=1):
            ark = fascicule["ark"]
            enregistrement = {"ark": ark, "annee": fascicule["annee"], "date": fascicule["date"]}
            try:
                enregistrement["pages"] = nombre_de_pages(
                    session, limiter, ark, args.timeout_seconds
                )
                if enregistrement["pages"] is None:
                    enregistrement["erreur"] = "nbVueImages absent"
                    echecs_consecutifs += 1
                else:
                    echecs_consecutifs = 0
            except Exception as exc:  # noqa: BLE001 - on journalise et on continue
                enregistrement["pages"] = None
                enregistrement["erreur"] = f"{exc.__class__.__name__}: {exc}"
                echecs_consecutifs += 1

            ledger.write(json.dumps(enregistrement, ensure_ascii=False) + "\n")
            ledger.flush()

            if index % 250 == 0 or index == len(restants):
                ecoule = time.time() - debut
                reste = (len(restants) - index) * (ecoule / index) / 3600
                print(
                    f"[pagination] {index}/{len(restants)} "
                    f"({index / len(restants) * 100:.1f} %) — reste ~{reste:.1f} h",
                    flush=True,
                )

            if echecs_consecutifs >= args.echecs_consecutifs_max:
                print(
                    f"[ARRET] {echecs_consecutifs} echecs consecutifs — Gallica "
                    f"refuse. Le ledger est intact, relancer plus tard reprendra "
                    f"ou on s'arrete.",
                    file=sys.stderr,
                )
                sys.exit(2)

    total = charger_ledger(chemin_pages)
    print(f"[fin] {len(total)}/{len(fascicules)} fascicules pagines.")


if __name__ == "__main__":
    main()
