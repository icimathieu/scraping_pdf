#!/usr/bin/env python3
"""Archive les manifests + inventaires de la presse sur HuggingFace.

Arborescence produite dans le dataset prive `icimathieu/corpus-memoire-ocr` :

    manifests_presse/<titre>.tar.zst          manifests synthetiques du titre
    manifests_presse/inventaires.tar.zst      les 18 inventaires JSONL compacts
    manifests_presse/README.md                ce que c'est, comment le relire

UN COMMIT PAR TITRE (18) + 2 = 20 commits au total, tres loin de la limite HF
de 128 commits/heure. Ne jamais descendre a la granularite du fichier : 211 000
manifests = 211 000 commits.

Idempotent : un ledger local (`state/hf_manifests_presse.txt`) retient les
titres deja pousses avec leur empreinte (nb de fichiers + taille). Un titre
inchange est saute ; un titre modifie est repousse. `--force` ignore le ledger.

Auth : token lu automatiquement depuis ~/.cache/huggingface/token (jamais de
variable d'environnement, jamais de token en clair). Si 401 : `hf auth login
--force` (le simple `hf auth login` repond "already logged in").

Exemple :
    python3 -u scripts/pipeline_presse/hf_backup_manifests_presse.py \
        --manifest-root manifests_presse \
        --inventaire-root input/inventaire_presse
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from huggingface_hub import HfApi
    from huggingface_hub.utils import HfHubHTTPError
except ImportError:  # message clair plutot qu'un traceback
    print("[ERREUR] huggingface_hub absent : uv pip install huggingface_hub",
          file=sys.stderr)
    sys.exit(1)

REPO = "icimathieu/corpus-memoire-ocr"
PREFIXE = "manifests_presse"
ZSTD_NIVEAU = "-19"
ZSTD_THREADS = 4

README = """# manifests_presse

Inventaire IIIF de la presse generaliste numerisee par Gallica, **1870-1914**,
18 titres. Produit par `scraping_pdf/scripts/pipeline_presse/`.

## Contenu

- `<titre>.tar.zst` — un manifest IIIF par numero, sous `<titre>/<numero_id>.manifest.json`.
- `inventaires.tar.zst` — un `<titre>.jsonl` par titre : une ligne par numero,
  champs `numero_id`, `ark`, `date`, `annee`, `pages`, `libelle`.
  **C'est ce fichier qu'il faut lire pour choisir les numeros a scraper** : il
  est minuscule et ne demande pas de detarer les manifests.

## Les manifests sont SYNTHETIQUES

Ils ne viennent pas de l'endpoint `manifest.json` de Gallica, plafonne a
1 req/min (211 000 numeros = 146 jours). Ils sont fabriques a partir du service
`Pagination` (30-40 req/min), dont le `nbVueImages` vaut exactement le nombre de
canvas, et de l'URL deterministe du service image (`/iiif/<ark>/f<n>`).

Ils contiennent donc le service image de chaque canvas — tout ce dont
`scraping_images_gallica.py` a besoin — mais **pas** les metadonnees
descriptives d'un vrai manifest. Le champ `_synthetique` le rappelle dans
chaque fichier.

## Pourquoi cet inventaire existe

Le corpus presse deja disponible (Europeana Newspapers, 18 titres) a des trous :
sur un echantillon de 6 couples (titre, annee), **31,8 % des numeros presents
sur Gallica manquent** cote Europeana, et certaines annees y sont entierement
absentes. Cet inventaire dit ce qui existe reellement chez Gallica, numero par
numero, pour pouvoir cibler ce qu'on scrape.
"""


def empreinte(dossier: Path) -> str:
    """Signature bon marche d'un dossier : nb de fichiers + octets cumules.
    Suffit a detecter un titre qui a bouge, sans hasher 15 000 fichiers."""
    n = taille = 0
    for racine, _d, fichiers in os.walk(dossier):
        for f in fichiers:
            try:
                taille += os.path.getsize(os.path.join(racine, f))
                n += 1
            except OSError:
                pass
    return f"{n}:{taille}"


def charger_ledger(chemin: Path) -> dict:
    connus = {}
    if chemin.exists():
        for ligne in chemin.read_text(encoding="utf-8").splitlines():
            if "\t" in ligne:
                cle, val = ligne.split("\t", 1)
                connus[cle] = val.strip()
    return connus


def compresser(source: Path, membres: list[str], dest: Path) -> None:
    """tar relatif a `source` -> dest.tar.zst. Passe par le shell pour le pipe
    tar|zstd, qui evite d'ecrire le .tar intermediaire sur disque."""
    args = " ".join(f"'{m}'" for m in membres)
    cmd = (f"tar -C '{source}' -cf - {args} "
           f"| zstd {ZSTD_NIVEAU} -T{ZSTD_THREADS} -q -o '{dest}'")
    subprocess.run(["bash", "-c", cmd], check=True)


def televerser(api: "HfApi", local: Path, chemin_repo: str, essais: int = 5) -> None:
    """Upload avec backoff sur 429 : la limite HF est de 128 commits/heure."""
    for tentative in range(1, essais + 1):
        try:
            api.upload_file(
                path_or_fileobj=str(local),
                path_in_repo=chemin_repo,
                repo_id=REPO,
                repo_type="dataset",
                commit_message=f"presse : {chemin_repo}",
            )
            return
        except HfHubHTTPError as exc:
            code = getattr(getattr(exc, "response", None), "status_code", None)
            if code == 429 and tentative < essais:
                pause = 60 * tentative
                print(f"    [429] limite HF atteinte, pause {pause}s "
                      f"(tentative {tentative}/{essais})", flush=True)
                time.sleep(pause)
                continue
            raise


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--manifest-root", default="manifests_presse")
    p.add_argument("--inventaire-root", default="input/inventaire_presse")
    p.add_argument("--ledger", default="state/hf_manifests_presse.txt")
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    racine = Path(args.manifest_root)
    inv = Path(args.inventaire_root)
    if not racine.is_dir():
        print(f"[ERREUR] {racine} introuvable — lancer construire_entree_presse.py "
              f"d'abord.", file=sys.stderr)
        sys.exit(1)

    ledger_path = Path(args.ledger)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    connus = {} if args.force else charger_ledger(ledger_path)

    titres = sorted(d.name for d in racine.iterdir() if d.is_dir())
    a_faire = [t for t in titres if connus.get(t) != empreinte(racine / t)]
    print(f"[hf] {len(titres)} titres, {len(a_faire)} a pousser "
          f"({len(titres) - len(a_faire)} inchanges)")

    if args.dry_run:
        for t in a_faire:
            print(f"  [DRY] {PREFIXE}/{t}.tar.zst")
        print(f"  [DRY] {PREFIXE}/inventaires.tar.zst")
        print(f"  [DRY] {PREFIXE}/README.md")
        return

    api = HfApi()  # lit ~/.cache/huggingface/token
    pousses = 0
    with tempfile.TemporaryDirectory() as tmp:
        tmpd = Path(tmp)

        for titre in a_faire:
            dest = tmpd / f"{titre}.tar.zst"
            compresser(racine, [titre], dest)
            mo = dest.stat().st_size / 1e6
            televerser(api, dest, f"{PREFIXE}/{titre}.tar.zst")
            with ledger_path.open("a", encoding="utf-8") as fh:
                fh.write(f"{titre}\t{empreinte(racine / titre)}\n")
            dest.unlink()
            pousses += 1
            print(f"[UP] {PREFIXE}/{titre}.tar.zst  {mo:.1f} Mo  "
                  f"({pousses}/{len(a_faire)})", flush=True)

        # Les inventaires sont minuscules et changent en bloc : une archive,
        # reecrite a chaque run.
        if inv.is_dir():
            dest = tmpd / "inventaires.tar.zst"
            compresser(inv.parent, [inv.name], dest)
            televerser(api, dest, f"{PREFIXE}/inventaires.tar.zst")
            print(f"[UP] {PREFIXE}/inventaires.tar.zst  "
                  f"{dest.stat().st_size / 1e6:.2f} Mo", flush=True)

        readme = tmpd / "README.md"
        readme.write_text(README, encoding="utf-8")
        televerser(api, readme, f"{PREFIXE}/README.md")
        print(f"[UP] {PREFIXE}/README.md")

    print(f"[FIN] {pousses} titre(s) pousse(s), "
          f"{pousses + 2} commit(s) — limite HF : 128/h")


if __name__ == "__main__":
    main()
