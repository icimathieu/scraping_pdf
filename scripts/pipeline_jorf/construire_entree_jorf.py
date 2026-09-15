#!/usr/bin/env python3
"""Etape 2 de la pipeline JORF : deduplication + manifests synthetiques.

Transforme l'inventaire produit par `inventaire_jorf.py` en une entree directement
consommable par `scripts/pipeline_manifest_iiif/scraping_images_gallica.py`,
**sans modifier ce dernier**.

Deduplication
-------------
Gallica publie 2 a 4 ARK pour la meme date : ce sont des **numerisations
multiples du meme fascicule**, pas des editions differentes. Verifie le
2026-09-06 sur le 15/10/1870, dont les 3 ARK portent le meme numero (A2,N284)
et la meme cote BnF (2010-217349) ; et par comptage de pages, identique sur
13 paires sur 14.

On garde donc **un ARK par date**, celui qui a **le plus de pages** — pour ne
jamais retenir une numerisation tronquee. A egalite, on prefere la famille
`bpt6k209*`, presente sur 3 693 des 3 708 dates, puis l'ordre alphabetique
pour que le choix soit reproductible.

Manifests synthetiques
----------------------
L'URL du service image IIIF de Gallica est deterministe
(`https://gallica.bnf.fr/iiif/<ark>/f<n>`), et `nbVueImages` vaut exactement le
nombre de canvas. On fabrique donc un manifest minimal contenant uniquement ce
que `extract_canvas_image_service_id()` lit. Cela evite les >5 jours de
scraping des vrais manifests (endpoint limite a ~1 req/min).

⚠️ On n'ecrit **pas** de champ `manifest_path` dans les items : il
court-circuiterait `--manifest-root` (piege CLAUDE.md §15).
"""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

BASE_IIIF = "https://gallica.bnf.fr/iiif/ark:/12148"
FAMILLE_PREFEREE = "bpt6k209"


def charger_inventaire(chemin_fascicules: Path, chemin_pages: Path) -> List[Dict]:
    fascicules = json.loads(chemin_fascicules.read_text(encoding="utf-8"))["fascicules"]
    pages_par_ark: Dict[str, int] = {}
    with chemin_pages.open("r", encoding="utf-8") as fh:
        for ligne in fh:
            ligne = ligne.strip()
            if not ligne:
                continue
            try:
                enr = json.loads(ligne)
            except json.JSONDecodeError:
                continue
            if enr.get("pages") is not None:
                pages_par_ark[enr["ark"]] = enr["pages"]
    for fascicule in fascicules:
        fascicule["pages"] = pages_par_ark.get(fascicule["ark"])
    return fascicules


def choisir(candidats: List[Dict]) -> Dict:
    """Le plus de pages ; a egalite, la famille preferee ; puis l'ordre alphabetique."""
    return sorted(
        candidats,
        key=lambda f: (
            -(f["pages"] or 0),
            0 if f["ark"].startswith(FAMILLE_PREFEREE) else 1,
            f["ark"],
        ),
    )[0]


def manifest_synthetique(ark: str, pages: int) -> Dict:
    ark_court = ark.split("/")[-1]
    return {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": f"{BASE_IIIF}/{ark_court}/manifest.json",
        "@type": "sc:Manifest",
        "_synthetique": (
            "Genere par construire_entree_jorf.py depuis le service Pagination "
            "de Gallica. nbVueImages == nombre de canvas (verifie 4/4). "
            "Ne contient que le service image de chaque canvas."
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fascicules", default="input/jorf_fascicules.json")
    parser.add_argument("--pagination", default="input/jorf_pagination.jsonl")
    parser.add_argument("--out-items", default="input/jorf_numeros.json")
    parser.add_argument("--manifest-root", default="manifest_jorf")
    parser.add_argument("--revue", default="jorf")
    parser.add_argument(
        "--exclure",
        default="",
        help="Fichier texte d'ARK a exclure (un par ligne, # = commentaire). "
             "Sert a ecarter nommement les tables annuelles reperees.",
    )
    parser.add_argument(
        "--seuil-outlier",
        type=float,
        default=4.0,
        help="Un fascicule dont le nombre de pages depasse ce facteur x la mediane "
             "de son annee est signale comme table probable (signale, pas exclu).",
    )
    parser.add_argument("--sans-manifests", action="store_true",
                        help="N'ecrit que le JSON d'items (diagnostic).")
    args = parser.parse_args()

    exclus = set()
    if args.exclure and Path(args.exclure).exists():
        for ligne in Path(args.exclure).read_text(encoding="utf-8").splitlines():
            ligne = ligne.split("#")[0].strip()
            if ligne:
                exclus.add(ligne)

    fascicules = charger_inventaire(Path(args.fascicules), Path(args.pagination))
    sans_pagination = [f for f in fascicules if f["pages"] is None]
    sans_date = [f for f in fascicules if f["date"] is None]

    par_date: Dict[str, List[Dict]] = defaultdict(list)
    for fascicule in fascicules:
        if fascicule["date"] and fascicule["pages"] is not None:
            par_date[fascicule["date"]].append(fascicule)

    retenus = [choisir(v) for _, v in sorted(par_date.items())]
    retenus = [f for f in retenus if f["ark"] not in exclus]

    # Reperage des tables probables : outliers de pagination dans leur annee.
    pages_par_annee: Dict[int, List[int]] = defaultdict(list)
    for fascicule in retenus:
        pages_par_annee[fascicule["annee"]].append(fascicule["pages"])
    medianes = {a: statistics.median(v) for a, v in pages_par_annee.items()}
    outliers = [
        f for f in retenus
        if f["pages"] > args.seuil_outlier * medianes[f["annee"]]
    ]

    items = []
    racine_manifests = Path(args.manifest_root) / args.revue
    if not args.sans_manifests:
        racine_manifests.mkdir(parents=True, exist_ok=True)

    for fascicule in retenus:
        ark_court = fascicule["ark"].split("/")[-1]
        numero_id = f"{args.revue}{fascicule['date'].replace('-', '')}_{ark_court}"
        alternatifs = [
            {"ark": f["ark"], "pages": f["pages"]}
            for f in sorted(par_date[fascicule["date"]], key=lambda x: x["ark"])
            if f["ark"] != ark_court
        ]
        items.append(
            {
                "revue": args.revue,
                "numero_id": numero_id,
                "issue_ark": f"ark:/12148/{ark_court}",
                "date": fascicule["date"],
                "year": fascicule["annee"],
                "pages_total": fascicule["pages"],
                # Numerisations concurrentes de la MEME date, gardees comme
                # filet de securite pour la passe de reparation : si une vue du
                # primaire reste introuvable APRES convergence du scraper, on
                # tente l'alternative avant de la declarer perdue.
                # ⚠️ Ne pas confondre un trou avec du throttling : Gallica
                # repond 404 (et pas 429) quand on le sollicite trop vite, et
                # ces 404-la sont transitoires. Verifie le 2026-09-06 : une
                # rafale de sondes a rendu 404 les dernieres vues de 8 ARK sur
                # 8, toutes revenues en 200 apres 10 min de pause. Un trou n'est
                # reel qu'apres convergence a 4 img/min, jamais sur une sonde.
                "arks_alternatifs": alternatifs,
                "status": "a_traiter",
                "pipeline_status": "",
            }
        )
        if not args.sans_manifests:
            (racine_manifests / f"{numero_id}.manifest.json").write_text(
                json.dumps(manifest_synthetique(ark_court, fascicule["pages"])),
                encoding="utf-8",
            )

    Path(args.out_items).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_items).write_text(
        json.dumps(
            {
                "source": "Journal officiel de la Republique francaise, ark:/12148/cb328020909",
                "periode": "1870-09-05 / 1880-12-31",
                "total_arks_gallica": len(fascicules),
                "total_numeros": len(items),
                "total_pages": sum(i["pages_total"] for i in items),
                "items": items,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    total_pages = sum(i["pages_total"] for i in items)
    print(f"ARK Gallica              : {len(fascicules)}")
    print(f"  sans pagination        : {len(sans_pagination)}")
    print(f"  sans date de jour      : {len(sans_date)} (tables probables, ecartes)")
    print(f"  exclus nommement       : {len(exclus)}")
    print(f"Numeros retenus          : {len(items)}")
    print(f"Pages a scraper          : {total_pages:,}".replace(",", " "))
    print(f"Duree a 4 img/min        : {total_pages / 4 / 60 / 24:.1f} jours")
    print(f"Poids estime (1,18 Mo/p) : {total_pages * 1.18 / 1000:.0f} Go")
    if not args.sans_manifests:
        print(f"Manifests synthetiques   : {racine_manifests}")
    if outliers:
        print(f"\n⚠️  {len(outliers)} outliers de pagination (tables probables, "
              f"> {args.seuil_outlier}x la mediane de leur annee) :")
        for f in sorted(outliers, key=lambda x: -x["pages"])[:25]:
            print(f"    {f['date']}  {f['pages']:>4} p.  {f['ark']}  "
                  f"(mediane {medianes[f['annee']]:.0f})")
        print("    -> verifier via OAIRecord, puis les lister dans --exclure.")
    if sans_date:
        print(f"\nFascicules sans date de jour (ecartes) :")
        for f in sans_date:
            print(f"    {f['libelle']!r}  {f['pages']} p.  {f['ark']}")


if __name__ == "__main__":
    main()
