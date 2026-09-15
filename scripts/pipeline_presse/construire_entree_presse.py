#!/usr/bin/env python3
"""Construit, HORS LIGNE, l'entree de scraping de la presse generaliste.

Consomme les deux sorties de `inventaire_presse.py` (`presse_fascicules.json` +
`presse_pagination.jsonl`) et produit :

  1. `input/presse_numeros.json` — l'entree que `scraping_images_gallica.py`
     avale telle quelle, quand on decidera quels numeros scraper ;
  2. `manifests_presse/<titre>/<numero_id>.manifest.json` — un manifest IIIF
     SYNTHETIQUE par numero. L'URL du service image de Gallica etant
     deterministe (`/iiif/<ark>/f<n>`) et `nbVueImages` valant exactement le
     nombre de canvas, ces manifests sont equivalents aux vrais pour notre usage
     et coutent 0 requete (les vrais plafonnent a 1 req/min = 146 jours) ;
  3. `input/inventaire_presse/<titre>.jsonl` — l'inventaire compact
     (ark, date, numero_id, pages). C'est LUI qu'on relira pour choisir les
     numeros a scraper : pas besoin de detarer les manifests.

Aucun appel reseau. Rejouable a volonte.

Exemple :
    python3 -u scripts/pipeline_presse/construire_entree_presse.py \
        --fascicules input/presse_fascicules.json \
        --pagination input/presse_pagination.jsonl \
        --out input/presse_numeros.json \
        --manifest-root manifests_presse \
        --inventaire-root input/inventaire_presse
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

BASE_IIIF = "https://gallica.bnf.fr/iiif/ark:/12148"


def manifest_synthetique(ark: str, pages: int) -> Dict:
    ark_court = ark.split("/")[-1]
    return {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": f"{BASE_IIIF}/{ark_court}/manifest.json",
        "@type": "sc:Manifest",
        "_synthetique": (
            "Genere par construire_entree_presse.py depuis le service Pagination "
            "de Gallica. nbVueImages == nombre de canvas. Ne contient que le "
            "service image de chaque canvas."
        ),
        "sequences": [
            {
                "@type": "sc:Sequence",
                "canvases": [
                    {
                        "@id": f"{BASE_IIIF}/{ark_court}/canvas/f{n}",
                        "@type": "sc:Canvas",
                        "label": f"f{n}",
                        "images": [
                            {
                                "@type": "oa:Annotation",
                                "resource": {
                                    "@type": "dctypes:Image",
                                    "service": {
                                        "@context": "http://iiif.io/api/image/2/context.json",
                                        "@id": f"{BASE_IIIF}/{ark_court}/f{n}",
                                        "profile": "http://iiif.io/api/image/2/level2.json",
                                    },
                                },
                            }
                        ],
                    }
                    for n in range(1, pages + 1)
                ],
            }
        ],
    }


def numero_id(slug: str, date: str | None, ark: str) -> str:
    """Identifiant stable et UNIQUE. L'ARK y figure parce que plusieurs numeros
    peuvent partager une date (supplements, editions multiples, numerisations
    doublonnees) : la date seule ne suffit pas."""
    jour = (date or "sansdate").replace("-", "")
    return f"{slug}_{jour}_{ark.split('/')[-1]}"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fascicules", default="input/presse_fascicules.json")
    p.add_argument("--pagination", default="input/presse_pagination.jsonl")
    p.add_argument("--out", default="input/presse_numeros.json")
    p.add_argument("--manifest-root", default="manifests_presse")
    p.add_argument("--inventaire-root", default="input/inventaire_presse")
    p.add_argument("--sans-manifests", action="store_true",
                   help="N'ecrit que l'entree et les inventaires (diagnostic).")
    p.add_argument("--seuil-outlier", type=float, default=4.0,
                   help="Signale les numeros a plus de N x la mediane de leur "
                        "titre (candidats tables annuelles). SIGNALE seulement, "
                        "n'exclut rien : on ne scrape pas encore les images.")
    args = p.parse_args()

    fascicules = json.loads(Path(args.fascicules).read_text(encoding="utf-8"))["fascicules"]
    par_ark = {f["ark"]: f for f in fascicules}

    pages: Dict[str, int] = {}
    erreurs = 0
    with Path(args.pagination).open(encoding="utf-8") as fh:
        for ligne in fh:
            ligne = ligne.strip()
            if not ligne:
                continue
            try:
                e = json.loads(ligne)
            except json.JSONDecodeError:
                continue
            if e.get("pages") is not None:
                pages[e["ark"]] = int(e["pages"])
            else:
                erreurs += 1

    manquants = [a for a in par_ark if a not in pages]
    print(f"[entree] {len(par_ark)} numeros enumeres, {len(pages)} pagines, "
          f"{len(manquants)} sans pagination, {erreurs} ligne(s) en erreur")

    items: List[Dict] = []
    par_titre: Dict[str, List[Dict]] = defaultdict(list)
    dates_vues: Dict[str, set] = defaultdict(set)
    doublons_date = 0

    for ark, f in par_ark.items():
        if ark not in pages:
            continue
        slug = f["titre"]
        nid = numero_id(slug, f["date"], ark)
        cle_date = f"{slug}|{f['date']}"
        if f["date"] and cle_date in dates_vues:
            doublons_date += 1
        dates_vues[cle_date].add(ark)
        item = {
            "revue": slug,                 # nom du champ attendu par le scraper
            "numero_id": nid,
            "issue_ark": ark,
            "date": f["date"],
            "libelle": f["libelle"],
            "year": f["annee"],
            "pages_total": pages[ark],
            "status": "a_traiter",
            "pipeline_status": "a_traiter",
            # PAS de "manifest_path" : il court-circuiterait --manifest-root
            # (piege documente dans CLAUDE.md §15).
        }
        items.append(item)
        par_titre[slug].append(item)

    items.sort(key=lambda i: (i["revue"], i["date"] or "", i["numero_id"]))

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(
        json.dumps(
            {
                "source": "inventaire_presse.py (services Issues + Pagination)",
                "total": len(items),
                "total_pages": sum(i["pages_total"] for i in items),
                "items": items,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    # --- inventaires compacts par titre --------------------------------------
    inv_root = Path(args.inventaire_root)
    inv_root.mkdir(parents=True, exist_ok=True)
    for slug, lot in sorted(par_titre.items()):
        with (inv_root / f"{slug}.jsonl").open("w", encoding="utf-8") as fh:
            for i in sorted(lot, key=lambda x: (x["date"] or "", x["numero_id"])):
                fh.write(json.dumps(
                    {"numero_id": i["numero_id"], "ark": i["issue_ark"],
                     "date": i["date"], "annee": i["year"],
                     "pages": i["pages_total"], "libelle": i["libelle"]},
                    ensure_ascii=False) + "\n")

    # --- manifests synthetiques ----------------------------------------------
    if not args.sans_manifests:
        root = Path(args.manifest_root)
        ecrits = 0
        for i in items:
            d = root / i["revue"]
            d.mkdir(parents=True, exist_ok=True)
            (d / f"{i['numero_id']}.manifest.json").write_text(
                json.dumps(manifest_synthetique(i["issue_ark"], i["pages_total"]),
                           ensure_ascii=False),
                encoding="utf-8",
            )
            ecrits += 1
        print(f"[manifests] {ecrits} manifests synthetiques ecrits sous {root}/")

    # --- bilan ----------------------------------------------------------------
    print(f"\n[bilan] {len(items)} numeros, "
          f"{sum(i['pages_total'] for i in items):,} pages".replace(",", " "))
    if doublons_date:
        print(f"[bilan] {doublons_date} numero(s) partagent une date avec un autre "
              f"(supplements, editions multiples ou numerisations doublonnees) — "
              f"conserves, l'ARK distingue.")
    print(f"\n{'titre':52} {'numeros':>8} {'pages':>9} {'p/num':>6}  outliers")
    print("-" * 96)
    for slug, lot in sorted(par_titre.items()):
        pp = [i["pages_total"] for i in lot]
        med = statistics.median(pp)
        gros = [i for i in lot if i["pages_total"] > args.seuil_outlier * med]
        ex = ", ".join(f"{g['date']}({g['pages_total']}p)" for g in
                       sorted(gros, key=lambda g: -g["pages_total"])[:2])
        print(f"{slug:52} {len(lot):8} {sum(pp):9} {med:6.0f}  "
              f"{len(gros):>3}{'  ' + ex if ex else ''}")
    if manquants:
        print(f"\n[ATTENTION] {len(manquants)} numeros enumeres n'ont pas de "
              f"pagination — relancer inventaire_presse.py pour les rattraper.")


if __name__ == "__main__":
    main()
