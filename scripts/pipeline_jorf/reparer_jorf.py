#!/usr/bin/env python3
"""Etape 3 (optionnelle) : reparer les pages manquantes via l'autre numerisation.

A lancer **seulement apres convergence du scrape** (`run_jorf.sh` a pose `DONE`,
ou deux passes consecutives donnent le meme residuel). Avant ça, une page
absente n'est pas un trou : c'est soit une page pas encore tentee, soit un 404
de throttling que la passe suivante recuperera.

⚠️ Gallica repond **404, pas 429**, quand on le sollicite trop vite (verifie le
2026-09-06 : une rafale de sondes a rendu 404 les dernieres vues de 8 ARK sur 8,
toutes revenues en 200 apres 10 min de pause). Ne jamais conclure a un trou sur
une sonde rapprochee.

Principe
--------
Chaque date a 2 a 4 numerisations du meme fascicule (meme numero, meme cote
BnF). Si le primaire ne sert pas une vue, l'alternative la sert peut-etre. On
reutilise **le meme `numero_id`** : le scraper saute les pages deja presentes
sur disque et ne telecharge que les manquantes, depuis l'autre exemplaire.

Garde-fou d'alignement
----------------------
On ne repare **que** depuis une alternative ayant **exactement le meme nombre
de pages** que le primaire. Sinon rien ne garantit que `fN` designe la meme
page dans les deux numerisations, et on melangerait deux paginations dans un
meme dossier. Les cas non alignes sont listes pour arbitrage manuel, jamais
reparcs en silence.
"""

import argparse
import json
from pathlib import Path

from construire_entree_jorf import manifest_synthetique


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--items", default="input/jorf_numeros.json")
    parser.add_argument("--image-root", required=True)
    parser.add_argument("--out-items", default="input/jorf_reparation.json")
    parser.add_argument("--manifest-root", required=True,
                        help="Les manifests de reparation y sont ECRASES (meme numero_id).")
    parser.add_argument("--extension", default=".jpg")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = json.loads(Path(args.items).read_text(encoding="utf-8"))
    image_root = Path(args.image_root)

    a_reparer, non_alignes, complets = [], [], 0
    for item in payload["items"]:
        dossier = image_root / item["revue"] / item["numero_id"]
        presentes = (
            sum(1 for f in dossier.iterdir() if f.name.endswith(args.extension))
            if dossier.is_dir() else 0
        )
        if presentes >= item["pages_total"]:
            complets += 1
            continue

        alignes = [
            a for a in item.get("arks_alternatifs", [])
            if a.get("pages") == item["pages_total"]
        ]
        manque = item["pages_total"] - presentes
        if not alignes:
            non_alignes.append((item, presentes, manque))
            continue

        remplacant = alignes[0]["ark"]
        a_reparer.append(
            {
                **{k: v for k, v in item.items() if k != "arks_alternatifs"},
                "issue_ark": f"ark:/12148/{remplacant}",
                "ark_primaire_remplace": item["issue_ark"],
                "pages_manquantes": manque,
                "status": "a_traiter",
                "pipeline_status": "",
            }
        )

    print(f"Numeros complets            : {complets}")
    print(f"Numeros reparables          : {len(a_reparer)} "
          f"({sum(i['pages_manquantes'] for i in a_reparer)} pages)")
    print(f"Incomplets SANS alternative alignee : {len(non_alignes)}")
    for item, presentes, manque in non_alignes[:20]:
        alt = ", ".join(f"{a['ark']}({a['pages']}p)" for a in item.get("arks_alternatifs", [])) or "aucune"
        print(f"    {item['numero_id']}  {presentes}/{item['pages_total']} "
              f"(-{manque})  alternatives: {alt}")

    if args.dry_run:
        print("\n(dry-run : rien ecrit)")
        return

    racine = Path(args.manifest_root)
    for item in a_reparer:
        ark_court = item["issue_ark"].split("/")[-1]
        chemin = racine / item["revue"] / f"{item['numero_id']}.manifest.json"
        chemin.parent.mkdir(parents=True, exist_ok=True)
        chemin.write_text(
            json.dumps(manifest_synthetique(ark_court, item["pages_total"])),
            encoding="utf-8",
        )

    Path(args.out_items).write_text(
        json.dumps(
            {
                "source": "reparation JORF depuis les numerisations alternatives",
                "total_numeros": len(a_reparer),
                "items": a_reparer,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nEcrit : {args.out_items} ({len(a_reparer)} numeros) "
          f"+ manifests de reparation dans {racine}")
    print("Relancer ensuite le scraper sur ce fichier, meme --image-root : "
          "les pages deja presentes sont sautees, seules les manquantes sont "
          "tirees depuis l'autre exemplaire.")


if __name__ == "__main__":
    main()
