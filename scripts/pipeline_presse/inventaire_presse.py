#!/usr/bin/env python3
"""Inventaire des numeros de presse generaliste sur Gallica (1870-1914).

Deux phases, la seconde reprenable :

  1. ENUMERATION — pour chaque titre, `services/Issues` liste les annees puis,
     annee par annee, les ARK des numeros. ~600 requetes au total.

  2. PAGINATION — pour chaque numero, `services/Pagination` donne son
     `nbVueImages`. C'est le gros du travail (~211 000 requetes).

Pourquoi pas les manifests IIIF ? Parce que cet endpoint plafonne a 1 req/min :
211 000 numeros = 146 jours. `Pagination` tient 30-40 req/min (mesure du
2026-09-06 sur le JORF : 40/min, 0 x 429 sur 45 requetes ; puis 4 h 15 de
production a 30/min, 0 erreur) et vaut EXACTEMENT le nombre de canvas du
manifest (verifie 4/4 sur le JORF). Comme l'URL du service image de Gallica est
deterministe (`/iiif/<ark>/f<n>`), `construire_entree_presse.py` fabrique
ensuite des manifests synthetiques que `scraping_images_gallica.py` avale sans
modification. Voir pipeline_presse.md.

Le ledger JSONL de la phase 2 rend le run reprenable : seuls les SUCCES y
comptent comme connus, un ARK en erreur est donc retente au run suivant.

Exemple :
    python3 -u scripts/pipeline_presse/inventaire_presse.py \
        --titres input/presse_titres.json \
        --out-issues input/presse_fascicules.json \
        --out-pages input/presse_pagination.jsonl \
        --requests-per-minute 30
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ISSUES_URL = "https://gallica.bnf.fr/services/Issues"
PAGINATION_URL = "https://gallica.bnf.fr/services/Pagination"

NB_VUES_PATTERN = re.compile(r"<nbVueImages>(\d+)</nbVueImages>")
DATE_PATTERN = re.compile(r"^(\d{1,2})\s+([A-Za-zéûîàôç]+)\s+(\d{4})$")

MOIS = {
    "janvier": 1, "fevrier": 2, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "aout": 8, "août": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "decembre": 12, "décembre": 12,
}

# Gallica refuse les /services/* sans User-Agent de navigateur (HTTP 403).
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


class Erreur429(Exception):
    """Gallica throttle. Distinguee des autres erreurs : elle merite un cooldown
    long et immediat, pas un simple retry."""

    def __init__(self, retry_after: float | None = None) -> None:
        super().__init__("HTTP 429")
        self.retry_after = retry_after


class CircuitBreaker:
    """Trois niveaux, calques sur ceux de scraping_images_gallica.py :

      - un 429 declenche un cooldown LONG et IMMEDIAT (on fuit le throttle) ;
      - N echecs transitoires consecutifs (5xx, timeout) declenchent un cooldown
        court ;
      - au-dela de M cooldowns, on s'arrete net plutot que de marteler.

    Le ledger etant ecrit et flushe a chaque requete, un arret est sans douleur :
    la relance reprend exactement ou on s'etait arrete.
    """

    def __init__(self, seuil: int, sleep_court: int, sleep_429: int,
                 cooldowns_max: int) -> None:
        self.seuil = seuil
        self.sleep_court = sleep_court
        self.sleep_429 = sleep_429
        self.cooldowns_max = cooldowns_max
        self.consecutifs = 0
        self.cooldowns = 0
        self.total_429 = 0
        self.total_erreurs = 0

    def succes(self) -> None:
        self.consecutifs = 0

    def echec(self, exc: Exception) -> bool:
        """True si on doit s'arreter definitivement."""
        self.total_erreurs += 1
        est_429 = isinstance(exc, Erreur429)
        if est_429:
            self.total_429 += 1
        self.consecutifs += 1

        if est_429 or self.consecutifs >= self.seuil:
            self.cooldowns += 1
            if self.cooldowns > self.cooldowns_max:
                print(f"[CB] {self.cooldowns - 1} cooldowns deja consommes — "
                      f"arret definitif.", file=sys.stderr, flush=True)
                return True
            pause = self.sleep_429 if est_429 else self.sleep_court
            if est_429 and getattr(exc, "retry_after", None):
                pause = max(pause, int(exc.retry_after))
            motif = "429 Gallica" if est_429 else f"{self.consecutifs} echecs consecutifs"
            print(f"[CB] {motif} — cooldown {pause}s "
                  f"({self.cooldowns}/{self.cooldowns_max})", flush=True)
            time.sleep(pause)
            self.consecutifs = 0
        return False


def build_session(user_agent: str) -> requests.Session:
    # 429 VOLONTAIREMENT ABSENT du status_forcelist : si urllib3 le retente
    # tout seul, le throttling de Gallica devient invisible et on ne cooldowne
    # jamais. On garde les 5xx, qui sont du bruit reseau sans signification.
    retry = Retry(
        total=4, connect=4, read=4, backoff_factor=2.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parse_date_fr(libelle: str) -> str | None:
    """'05 septembre 1870' -> '1870-09-05'. None si le libelle ne porte pas de
    date de jour (annee seule = typiquement une table annuelle)."""
    match = DATE_PATTERN.match(libelle.strip())
    if not match:
        return None
    jour, mois, annee = match.groups()
    numero_mois = MOIS.get(mois.lower())
    if numero_mois is None:
        return None
    return f"{int(annee):04d}-{numero_mois:02d}-{int(jour):02d}"


def enumerer_titre(
    session: requests.Session,
    limiter: RateLimiter,
    slug: str,
    ark_titre: str,
    start_year: int,
    end_year: int,
    timeout: int,
) -> List[Dict]:
    """Enumere les numeros d'UN titre sur la periode. 1 + N requetes."""
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
    if not annees:
        print(f"[issues][{slug}] AUCUNE annee dans {start_year}-{end_year}", flush=True)
        return []

    fascicules: List[Dict] = []
    for annee in annees:
        limiter.wait_turn()
        reponse = session.get(
            ISSUES_URL, params={"ark": ark_titre, "date": str(annee)}, timeout=timeout
        )
        reponse.raise_for_status()
        for noeud in ET.fromstring(reponse.text).findall(".//issue"):
            ark = noeud.attrib.get("ark", "").strip()
            if not ark:
                continue
            libelle = (noeud.text or "").strip()
            fascicules.append(
                {
                    "titre": slug,
                    "ark": ark,
                    "annee": annee,
                    "day_of_year": noeud.attrib.get("dayOfYear", "").strip(),
                    "libelle": libelle,
                    "date": parse_date_fr(libelle),
                }
            )
    print(
        f"[issues][{slug}] {len(annees)} annees ({annees[0]}-{annees[-1]}), "
        f"{len(fascicules)} numeros",
        flush=True,
    )
    return fascicules


def charger_ledger(chemin: Path) -> Dict[str, Dict]:
    """Relit le JSONL de la phase 2. Une ligne corrompue (run tue en pleine
    ecriture) est ignoree plutot que de faire echouer la reprise. Seul un SUCCES
    compte comme connu : un ARK en erreur sera retente au run suivant."""
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
            if ark and enregistrement.get("pages") is not None:
                connus[ark] = enregistrement
    return connus


def nombre_de_pages(
    session: requests.Session, limiter: RateLimiter, ark: str, timeout: int
) -> int | None:
    limiter.wait_turn()
    reponse = session.get(PAGINATION_URL, params={"ark": ark}, timeout=timeout)
    if reponse.status_code == 429:
        ra = reponse.headers.get("Retry-After")
        raise Erreur429(float(ra) if (ra or "").strip().isdigit() else None)
    reponse.raise_for_status()
    match = NB_VUES_PATTERN.search(reponse.text)
    return int(match.group(1)) if match else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--titres", default="input/presse_titres.json")
    parser.add_argument("--start-year", type=int, default=1870)
    parser.add_argument("--end-year", type=int, default=1914)
    parser.add_argument("--out-issues", default="input/presse_fascicules.json")
    parser.add_argument("--out-pages", default="input/presse_pagination.jsonl")
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=30,
        help="30 par defaut : 40/min mesure sans 429 le 2026-09-06, on garde une "
             "marge (meme logique que RPM=4 sur l'API Image).",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--user-agent", default=UA_DEFAUT)
    parser.add_argument("--cb-threshold", type=int, default=8,
                        help="Echecs transitoires consecutifs avant cooldown court.")
    parser.add_argument("--cb-sleep-seconds", type=int, default=300,
                        help="Cooldown apres une rafale d'erreurs 5xx/timeout.")
    parser.add_argument("--cb-sleep-429-seconds", type=int, default=900,
                        help="Cooldown apres un 429 : long et immediat, on fuit "
                             "le throttle plutot que de le titiller.")
    parser.add_argument("--cb-max-cooldowns", type=int, default=6,
                        help="Au-dela, arret net. Le ledger est intact, la "
                             "relance reprend ou on s'est arrete.")
    parser.add_argument("--state-dir", default="",
                        help="Si fourni, y ecrit progress.txt (lu par le "
                             "watchdog et le digest quotidien).")
    parser.add_argument(
        "--seulement",
        default="",
        help="Slug(s) de titre separes par des virgules, pour un run partiel.",
    )
    args = parser.parse_args()

    chemin_titres = Path(args.titres)
    chemin_issues = Path(args.out_issues)
    chemin_pages = Path(args.out_pages)
    chemin_issues.parent.mkdir(parents=True, exist_ok=True)
    chemin_pages.parent.mkdir(parents=True, exist_ok=True)

    titres = json.loads(chemin_titres.read_text(encoding="utf-8"))["titres"]
    if args.seulement:
        voulus = {s.strip() for s in args.seulement.split(",") if s.strip()}
        titres = [t for t in titres if t["slug"] in voulus]
        if not titres:
            print(f"[ERREUR] aucun titre ne correspond a --seulement", file=sys.stderr)
            sys.exit(1)

    session = build_session(args.user_agent)
    limiter = RateLimiter(args.requests_per_minute)

    # --- Phase 1 : enumeration (rejouee seulement si le fichier manque) --------
    if chemin_issues.exists():
        charge = json.loads(chemin_issues.read_text(encoding="utf-8"))
        fascicules = charge["fascicules"]
        print(f"[issues] {len(fascicules)} numeros relus depuis {chemin_issues}")
    else:
        fascicules = []
        for titre in titres:
            fascicules.extend(
                enumerer_titre(
                    session, limiter, titre["slug"], titre["ark_titre"],
                    args.start_year, args.end_year, args.timeout_seconds,
                )
            )
        chemin_issues.write_text(
            json.dumps(
                {
                    "periode": [args.start_year, args.end_year],
                    "total_titres": len(titres),
                    "total": len(fascicules),
                    "fascicules": fascicules,
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        print(f"[issues] {len(fascicules)} numeros ecrits dans {chemin_issues}")

    # --- Phase 2 : pagination, reprenable -------------------------------------
    connus = charger_ledger(chemin_pages)
    restants = [f for f in fascicules if f["ark"] not in connus]
    print(
        f"[pagination] {len(connus)} deja connus, {len(restants)} a interroger "
        f"(~{len(restants) / max(1, args.requests_per_minute) / 60:.1f} h a "
        f"{args.requests_per_minute}/min)",
        flush=True,
    )

    breaker = CircuitBreaker(
        args.cb_threshold, args.cb_sleep_seconds,
        args.cb_sleep_429_seconds, args.cb_max_cooldowns,
    )
    etat = Path(args.state_dir) if args.state_dir else None
    if etat:
        etat.mkdir(parents=True, exist_ok=True)

    def ecrire_etat(faits: int) -> None:
        """Numerateur ET compteurs d'incidents, ecrits EN CONTINU. Jamais en fin
        de run : un digest branche sur un fichier de fin de course annonce 0
        pendant toute la campagne (piege CLAUDE.md §22, corrige le 07/09)."""
        if not etat:
            return
        (etat / "progress.txt").write_text(
            f"{len(connus) + faits} {len(fascicules)} {breaker.total_429} "
            f"{breaker.total_erreurs} {breaker.cooldowns} {int(time.time())}\n",
            encoding="utf-8",
        )

    debut = time.time()
    arret_cb = False
    ecrire_etat(0)
    with chemin_pages.open("a", encoding="utf-8") as ledger:
        for index, fascicule in enumerate(restants, start=1):
            ark = fascicule["ark"]
            enregistrement = {
                "ark": ark,
                "titre": fascicule["titre"],
                "annee": fascicule["annee"],
                "date": fascicule["date"],
            }
            try:
                enregistrement["pages"] = nombre_de_pages(
                    session, limiter, ark, args.timeout_seconds
                )
                if enregistrement["pages"] is None:
                    enregistrement["erreur"] = "nbVueImages absent"
                    arret_cb = breaker.echec(ValueError("nbVueImages absent"))
                else:
                    breaker.succes()
            except Exception as exc:  # noqa: BLE001 - on journalise et on continue
                enregistrement["pages"] = None
                enregistrement["erreur"] = f"{exc.__class__.__name__}: {exc}"
                arret_cb = breaker.echec(exc)

            ledger.write(json.dumps(enregistrement, ensure_ascii=False) + "\n")
            ledger.flush()

            if index % 100 == 0 or index == len(restants):
                ecrire_etat(index)
            if index % 500 == 0 or index == len(restants):
                ecoule = time.time() - debut
                reste = (len(restants) - index) * (ecoule / index) / 3600
                print(
                    f"[pagination] {index}/{len(restants)} "
                    f"({index / len(restants) * 100:.1f} %) — reste ~{reste:.1f} h "
                    f"— 429={breaker.total_429} err={breaker.total_erreurs} "
                    f"cooldowns={breaker.cooldowns}",
                    flush=True,
                )

            if arret_cb:
                ecrire_etat(index)
                print("[ARRET] circuit breaker — le ledger est intact, une "
                      "relance reprendra ou on s'est arrete.",
                      file=sys.stderr, flush=True)
                sys.exit(2)

    ecrire_etat(len(restants))
    print(f"[bilan] 429={breaker.total_429} erreurs={breaker.total_erreurs} "
          f"cooldowns={breaker.cooldowns}")
    print(f"[FIN] ledger complet : {chemin_pages}")


if __name__ == "__main__":
    main()
