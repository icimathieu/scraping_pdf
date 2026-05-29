"""
Post-traitement etape 2 : extrait pages_total de chaque manifest IIIF, joint au JSON
enrichi par scraping_manifest_gallica.py, affiche la distribution et propose un seuil
de partition PDF/IIIF.

Sortie :
- input/arks_numeros_with_manifests.json reecrit avec un champ `pages_total` par item.
- input/arks_partition_pdf.json   (items avec pages_total >= seuil)
- input/arks_partition_iiif.json  (items avec pages_total < seuil)

Le script n'envoie aucune requete reseau : il lit uniquement les fichiers locaux
sous manifest_iiif_process/.
"""

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional


def extract_pages_total(manifest_path: Path) -> Optional[int]:
    """Lit un manifest IIIF (v2 ou v3) et retourne le nombre de canvases."""
    if not manifest_path.is_file():
        return None
    try:
        with manifest_path.open("r", encoding="utf-8") as fh:
            m = json.load(fh)
    except Exception:
        return None
    # IIIF Presentation v2 : sequences[0].canvases
    sequences = m.get("sequences")
    if isinstance(sequences, list) and sequences:
        canvases = sequences[0].get("canvases")
        if isinstance(canvases, list):
            return len(canvases)
    # IIIF Presentation v3 : items (au niveau racine, ce sont les canvases)
    items = m.get("items")
    if isinstance(items, list):
        return len(items)
    return None


def quantile(sorted_values: List[int], q: float) -> int:
    if not sorted_values:
        return 0
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    pos = q * (len(sorted_values) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_values[lo]
    frac = pos - lo
    return int(round(sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac))


def text_histogram(values: List[int], bins: List[int]) -> List[str]:
    """Histogramme texte sur les bornes fournies (incluses a gauche)."""
    if not values:
        return ["(aucune donnee)"]
    counts = [0] * len(bins)
    labels = []
    for i, lo in enumerate(bins):
        hi = bins[i + 1] - 1 if i + 1 < len(bins) else None
        labels.append(f"[{lo:>5d} - {hi:>5d}]" if hi is not None else f"[{lo:>5d}+        ]")
    for v in values:
        placed = False
        for i in range(len(bins) - 1, -1, -1):
            if v >= bins[i]:
                counts[i] += 1
                placed = True
                break
        if not placed:
            counts[0] += 1
    max_count = max(counts) if counts else 1
    bar_width = 50
    lines = []
    for label, c in zip(labels, counts):
        bar = "#" * int(c / max_count * bar_width) if max_count > 0 else ""
        pct = c / len(values) * 100 if values else 0
        lines.append(f"  {label} : {c:>5d}  ({pct:5.1f}%)  {bar}")
    return lines


def propose_threshold(sorted_values: List[int]) -> Dict[str, Any]:
    """Propose plusieurs seuils candidats et calcule, pour chacun, la repartition."""
    candidates = [100, 150, 200, 250, 300, 400, 500, 750, 1000]
    n = len(sorted_values)
    total_pages = sum(sorted_values)
    result = []
    for thr in candidates:
        big = [v for v in sorted_values if v >= thr]
        small = [v for v in sorted_values if v < thr]
        result.append({
            "threshold": thr,
            "n_big_pdf": len(big),
            "n_small_iiif": len(small),
            "pct_big_pdf": (len(big) / n * 100) if n else 0,
            "pages_in_pdf": sum(big),
            "pages_in_iiif": sum(small),
            "pct_pages_in_pdf": (sum(big) / total_pages * 100) if total_pages else 0,
            # cout temps a rpm=4 IIIF (1 page = 1 req) : minutes
            "iiif_time_hours": (sum(small) / 4 / 60) if small else 0,
        })
    return {"candidates": result, "total_items": n, "total_pages": total_pages}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extrait pages_total des manifests IIIF et propose un seuil de partition."
    )
    parser.add_argument(
        "--input",
        default="input/arks_numeros_with_manifests.json",
        help="JSON enrichi produit par scraping_manifest_gallica.py.",
    )
    parser.add_argument(
        "--manifest-root",
        default="manifest_iiif_process",
        help="Racine des manifests IIIF.",
    )
    parser.add_argument(
        "--output",
        default="input/arks_numeros_with_manifests.json",
        help="Reecrit le JSON enrichi avec pages_total par item.",
    )
    parser.add_argument(
        "--partition-pdf-output",
        default="input/arks_partition_pdf.json",
    )
    parser.add_argument(
        "--partition-iiif-output",
        default="input/arks_partition_iiif.json",
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=None,
        help="Seuil pages_total pour la bascule PDF (>=) / IIIF (<). Si non fourni, "
             "seulement la distribution est affichee.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    manifest_root = Path(args.manifest_root)

    if not input_path.exists():
        raise SystemExit(f"Input introuvable: {input_path}")

    with input_path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise SystemExit("Payload 'items' invalide.")

    # 1. Enrichir chaque item avec pages_total
    enriched = 0
    missing = 0
    for it in items:
        mp_raw = it.get("manifest_path")
        if not mp_raw:
            missing += 1
            continue
        pages = extract_pages_total(Path(mp_raw))
        if pages is None:
            missing += 1
            continue
        it["pages_total"] = pages
        enriched += 1

    print(f"[INFO] enrichissement : {enriched} items avec pages_total, {missing} sans manifest exploitable")

    # 2. Distribution
    page_counts = sorted([it["pages_total"] for it in items if isinstance(it.get("pages_total"), int)])
    n = len(page_counts)
    if n == 0:
        print("[ERROR] aucune valeur pages_total. Verifier que les manifests existent sous manifest_iiif_process/")
        return

    print()
    print("=" * 70)
    print(f"DISTRIBUTION pages_total sur {n} items")
    print("=" * 70)
    print(f"  min       : {page_counts[0]}")
    print(f"  Q1 (25%)  : {quantile(page_counts, 0.25)}")
    print(f"  median    : {quantile(page_counts, 0.50)}")
    print(f"  mean      : {statistics.mean(page_counts):.1f}")
    print(f"  Q3 (75%)  : {quantile(page_counts, 0.75)}")
    print(f"  P90       : {quantile(page_counts, 0.90)}")
    print(f"  P95       : {quantile(page_counts, 0.95)}")
    print(f"  P99       : {quantile(page_counts, 0.99)}")
    print(f"  max       : {page_counts[-1]}")
    print(f"  total pgs : {sum(page_counts)}")
    print()
    print("Histogramme :")
    bins = [0, 20, 50, 100, 200, 300, 500, 750, 1000, 1500, 2000]
    for line in text_histogram(page_counts, bins):
        print(line)

    # 3. Candidats seuils
    print()
    print("=" * 70)
    print("CANDIDATS SEUIL PARTITION (PDF si pages_total >= seuil, IIIF sinon)")
    print("=" * 70)
    print(f"  {'seuil':>6} | {'#PDF':>6} {'%PDF':>6} | {'#IIIF':>6} | {'pages_PDF':>10} {'pages_IIIF':>11} | iiif_h@rpm=4")
    proposal = propose_threshold(page_counts)
    for c in proposal["candidates"]:
        print(
            f"  {c['threshold']:>6} | {c['n_big_pdf']:>6} {c['pct_big_pdf']:>5.1f}% | "
            f"{c['n_small_iiif']:>6} | {c['pages_in_pdf']:>10} {c['pages_in_iiif']:>11} | "
            f"{c['iiif_time_hours']:>10.0f}h"
        )

    print()
    print("Lecture : pour chaque seuil candidat :")
    print(" - #PDF = nb fascicules a router vers le pipeline PDF Selenium")
    print(" - #IIIF = nb fascicules a router vers le pipeline IIIF page-par-page")
    print(" - iiif_h@rpm=4 = temps total IIIF estime a 4 requetes/min (sequentiel)")

    # 4. Sauvegarde JSON enrichi
    payload["items"] = items
    payload["pages_total_stats"] = {
        "n_items_with_pages": n,
        "n_items_without_pages": missing,
        "min": page_counts[0],
        "max": page_counts[-1],
        "median": quantile(page_counts, 0.50),
        "p90": quantile(page_counts, 0.90),
        "total_pages": sum(page_counts),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print()
    print(f"[OK] JSON enrichi avec pages_total reecrit : {output_path}")

    # 5. Partition si seuil fourni
    if args.threshold is not None:
        thr = args.threshold
        pdf_items = [it for it in items if isinstance(it.get("pages_total"), int) and it["pages_total"] >= thr]
        iiif_items = [it for it in items if isinstance(it.get("pages_total"), int) and it["pages_total"] < thr]
        # On garde aussi les items sans pages_total dans iiif par defaut (a re-tenter)
        no_pages_items = [it for it in items if not isinstance(it.get("pages_total"), int)]

        pdf_payload = {"items": pdf_items, "partition": "pdf", "threshold_pages_total": thr,
                       "n_items": len(pdf_items)}
        iiif_payload = {"items": iiif_items + no_pages_items, "partition": "iiif",
                        "threshold_pages_total": thr,
                        "n_items": len(iiif_items) + len(no_pages_items),
                        "n_items_with_pages_lt_threshold": len(iiif_items),
                        "n_items_without_pages": len(no_pages_items)}

        Path(args.partition_pdf_output).write_text(
            json.dumps(pdf_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        Path(args.partition_iiif_output).write_text(
            json.dumps(iiif_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print()
        print(f"[OK] Partition seuil={thr} :")
        print(f"     PDF  ({len(pdf_items)} items) -> {args.partition_pdf_output}")
        print(f"     IIIF ({len(iiif_items) + len(no_pages_items)} items, dont {len(no_pages_items)} sans pages_total) -> {args.partition_iiif_output}")


if __name__ == "__main__":
    main()
