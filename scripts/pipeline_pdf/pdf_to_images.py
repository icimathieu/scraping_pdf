#!/usr/bin/env python3
"""Pipeline PDF - Etape 3 : PDF Gallica -> images de page, par EXTRACTION.

Remplace le rendu Poppler de `scraping_pdf_to_images.py`. Un PDF Gallica ne
contient pas une page a dessiner : il contient UNE image scannee par page. Le
rendu la decodait, la reechantillonnait a 200 DPI, puis la reencodait en PNG —
pour zero information ajoutee, le scan embarque etant le plafond de resolution
(cf. CLAUDE.md §16 : "les PDF Gallica embarquent des images ~140 ppi ; rendre
au-dessus interpole").

Ici on extrait le flux embarque TEL QUEL : les pixels sont bit-a-bit ceux que
Gallica a mis dans le PDF (verifie contre `pdfimages -j`, ecart de 1 octet de
marqueur de fin, pixels identiques).

Mesures sur `revue_industrielle` (2048x2882 JPEG couleur embarque) :

    pdftoppm -r 200  (ancien defaut)   55,30 s/page   16,00 Mo/page
    pdfimages -j                        0,91 s/page    1,80 Mo/page
    extraction pypdfium2 (ici)          0,03 s/page    1,86 Mo/page

`pypdfium2` est une wheel autonome (aucune dependance systeme) : c'est ce qui
permet a l'etape de tourner sur imac-server, ou poppler est absent et ou l'on
n'a pas de sudo sans mot de passe.

DEUX GARDE-FOUS, tous deux constates en mesurant :

1. **Plusieurs images sur la page.** `revue_industrielle1892002` porte 1 417
   images pour 1 411 pages (~0,4 % des pages). On ne saurait pas laquelle
   extraire -> on REND la page (pdfium compose alors la page complete).
2. **Rotation de page.** L'extraction ignore le `/Rotate` : une page a 90 deg
   sortirait couchee -> on la REND aussi.

Le rendu de repli se fait a la resolution NATIVE du scan embarque, jamais
au-dessus : sur-echantillonner ne fait que gonfler le fichier.

Sortie : `<images-root>/<revue>/<numero_id>/page_NNNN.<ext>`, meme convention
que la voie IIIF. L'extension suit le format embarque (.jpg pour du DCTDecode,
.png pour du CCITT/Flate decode par pdfium).

Reprise : basee sur la presence du fichier sur disque (comme le scraper IIIF),
aucun etat central.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import pypdfium2 as pdfium
except ImportError:  # message clair plutot qu'un traceback
    sys.exit(
        "pypdfium2 est requis : pip install pypdfium2\n"
        "(wheel autonome, aucune dependance systeme, aucun sudo)"
    )

# Sous ce seuil (cote long, en px) une image embarquee est un logo ou une
# vignette, pas un scan de page : le bandeau Gallica fait 642x251.
MIN_LONG_DEFAULT = 800


@dataclass
class NumeroResult:
    numero_id: str
    revue: str
    pages: int = 0
    extraites: int = 0
    rendues: int = 0
    sautees: int = 0
    erreurs: int = 0
    octets: int = 0
    secondes: float = 0.0
    raisons_rendu: Dict[str, int] = field(default_factory=dict)
    message: str = ""
    # Gabarit reel du scan embarque, releve au passage. La resolution varie
    # d'une ANNEE a l'autre AU SEIN d'une meme revue (agriculture_nationale_organe :
    # CCITT 2481x3508 jusqu'en 1906, puis JPEG gris 1024x1448) -> un releve par
    # revue est trop grossier pour decider quels numeros sont trop pauvres pour
    # l'OCR. On l'ecrit par numero, sans cout : les dimensions sont deja lues.
    cote_long_median: Optional[int] = None
    formats: Dict[str, int] = field(default_factory=dict)


def _page_images(page, min_long: int) -> List[Any]:
    """Images embarquees de la page dont le cote long depasse `min_long`."""
    out = []
    for obj in page.get_objects(filter=(pdfium.raw.FPDF_PAGEOBJ_IMAGE,)):
        try:
            w, h = obj.get_px_size()
        except Exception:
            continue
        if max(w, h) >= min_long:
            out.append(obj)
    return out


def _reference_long_px(doc, min_long: int, sample: int = 8) -> Optional[int]:
    """Cote long typique des scans du document, en px.

    Sert de cible aux pages SANS image embarquee (couvertures blanches,
    intercalaires) : sans reference on les rendrait a un DPI arbitraire, ce qui
    donnait 5689x8534 pour une page blanche. On veut qu'une page vide pese le
    meme gabarit que ses voisines, pas dix fois plus.
    """
    seen: List[int] = []
    total = len(doc)
    step = max(1, total // max(1, sample))
    for index in range(0, total, step):
        try:
            for obj in doc[index].get_objects(filter=(pdfium.raw.FPDF_PAGEOBJ_IMAGE,)):
                w, h = obj.get_px_size()
                if max(w, h) >= min_long:
                    seen.append(max(w, h))
                    break
        except Exception:
            continue
        if len(seen) >= sample:
            break
    if not seen:
        return None
    seen.sort()
    return seen[len(seen) // 2]


def _render_native(page, images: List[Any], dest_noext: Path,
                   reference_long_px: Optional[int]) -> Path:
    """Rend la page a la resolution native du scan embarque.

    pdfium rend a `scale` x 72 DPI. On calcule le scale qui reproduit le nombre
    de pixels du scan embarque : ni interpolation, ni perte de detail. Sans
    image sur la page, on vise le gabarit median du document (cf.
    `_reference_long_px`) plutot qu'un DPI arbitraire.
    """
    w_pt, h_pt = page.get_size()
    long_pt = max(w_pt, h_pt) or 1.0
    long_px = None
    if images:
        long_px = max(max(o.get_px_size()) for o in images)
    elif reference_long_px:
        long_px = reference_long_px
    scale = (long_px / long_pt) if long_px else 1.0
    scale = max(0.1, min(scale, 8.0))       # garde-fou anti-explosion memoire
    bitmap = page.render(scale=scale)
    pil = bitmap.to_pil()
    dest = dest_noext.with_suffix(".png")
    tmp = dest.with_name(dest.name + ".part")
    pil.save(tmp, format="PNG", compress_level=6)
    os.replace(tmp, dest)
    return dest


def _extract_passthrough(image, dest_noext: Path) -> Optional[Path]:
    """Ecrit le flux embarque tel quel. None si pdfium ne sait pas le sortir."""
    tmp_noext = dest_noext.with_name("." + dest_noext.name + ".part")
    for stale in tmp_noext.parent.glob(tmp_noext.name + ".*"):
        stale.unlink(missing_ok=True)
    try:
        image.extract(str(tmp_noext))       # pypdfium2 ajoute l'extension
    except Exception:
        for stale in tmp_noext.parent.glob(tmp_noext.name + ".*"):
            stale.unlink(missing_ok=True)
        return None
    produced = list(tmp_noext.parent.glob(tmp_noext.name + ".*"))
    if len(produced) != 1 or produced[0].stat().st_size == 0:
        for stale in produced:
            stale.unlink(missing_ok=True)
        return None
    dest = dest_noext.with_suffix(produced[0].suffix)
    os.replace(produced[0], dest)
    return dest


def _to_gray(path: Path) -> None:
    """Convertit sur place en 8-bit gris. Couteux : decode + reencode."""
    from PIL import Image

    with Image.open(path) as im:
        if im.mode == "L":
            return
        gray = im.convert("L")
        dest = path.with_suffix(".png")
        tmp = dest.with_name(dest.name + ".part")
        gray.save(tmp, format="PNG", compress_level=6)
    os.replace(tmp, dest)
    if dest != path:
        path.unlink(missing_ok=True)


def _profil_depuis_sortie(out_dir: Path) -> Tuple[Optional[int], Dict[str, int]]:
    """Gabarit d'un numero deja converti, lu sur ses images de sortie.

    Sert quand la passe n'a rien a faire (reprise) : sans ca, un numero converti
    lors d'un run anterieur n'aurait jamais son sidecar.
    """
    from PIL import Image

    formats: Dict[str, int] = {}
    longs: List[int] = []
    files = sorted(p for p in out_dir.glob("page_*") if p.suffix != ".part")
    for f in files:
        formats[f.suffix.lstrip(".")] = formats.get(f.suffix.lstrip("."), 0) + 1
    for f in files[:: max(1, len(files) // 8)][:8]:
        try:
            with Image.open(f) as im:
                longs.append(max(im.size))
        except Exception:
            continue
    longs.sort()
    return (longs[len(longs) // 2] if longs else None), formats


def _existing_page(out_dir: Path, stem: str) -> Optional[Path]:
    for cand in out_dir.glob(stem + ".*"):
        if cand.suffix != ".part" and cand.stat().st_size > 0:
            return cand
    return None


def convert_numero(
    pdf_path: Path,
    out_dir: Path,
    *,
    min_long: int,
    gray: bool,
    force: bool,
    delete_pdf: bool,
) -> NumeroResult:
    res = NumeroResult(numero_id=out_dir.name, revue=out_dir.parent.name)
    started = time.monotonic()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        doc = pdfium.PdfDocument(str(pdf_path))
    except Exception as exc:
        res.erreurs = 1
        res.message = f"{type(exc).__name__}: {exc}"
        return res

    longs: List[int] = []
    try:
        res.pages = len(doc)
        reference_long_px = _reference_long_px(doc, min_long)
        for index in range(res.pages):
            stem = f"page_{index + 1:04d}"
            if not force:
                already = _existing_page(out_dir, stem)
                if already is not None:
                    res.sautees += 1
                    res.octets += already.stat().st_size
                    continue
            dest_noext = out_dir / stem
            try:
                page = doc[index]
                images = _page_images(page, min_long)
                rotation = page.get_rotation()

                if images:
                    longs.append(max(max(o.get_px_size()) for o in images))

                written = None
                if len(images) == 1 and rotation == 0:
                    written = _extract_passthrough(images[0], dest_noext)
                    if written is not None:
                        res.extraites += 1
                if written is None:
                    if rotation:
                        raison = "rotation"
                    elif len(images) > 1:
                        raison = "images_multiples"
                    elif not images:
                        raison = "aucune_image"
                    else:
                        raison = "extraction_impossible"
                    res.raisons_rendu[raison] = res.raisons_rendu.get(raison, 0) + 1
                    written = _render_native(page, images, dest_noext, reference_long_px)
                    res.rendues += 1

                if gray:
                    _to_gray(written)
                    written = _existing_page(out_dir, stem) or written
                res.octets += written.stat().st_size
                ext = written.suffix.lstrip(".")
                res.formats[ext] = res.formats.get(ext, 0) + 1
            except Exception as exc:
                res.erreurs += 1
                if not res.message:
                    res.message = f"p{index + 1}: {type(exc).__name__}: {exc}"
    finally:
        doc.close()

    if longs:
        longs.sort()
        res.cote_long_median = longs[len(longs) // 2]
    elif res.sautees:
        # tout etait deja converti : on releve le gabarit sur une image de sortie
        res.cote_long_median, res.formats = _profil_depuis_sortie(out_dir)

    # Sidecar par numero : c'est lui qui permettra de trier les numeros trop
    # basse resolution AVANT de lancer l'OCR, sans rouvrir les PDF.
    try:
        (out_dir / "_conversion.json").write_text(
            json.dumps({
                "numero_id": res.numero_id, "revue": res.revue, "pages": res.pages,
                "extraites": res.extraites, "rendues": res.rendues,
                "raisons_rendu": res.raisons_rendu, "erreurs": res.erreurs,
                "cote_long_median": res.cote_long_median, "formats": res.formats,
                "mo_par_page": round(res.octets / 1e6 / max(res.pages, 1), 3),
            }, ensure_ascii=False, indent=1), encoding="utf-8")
    except OSError:
        pass

    res.secondes = time.monotonic() - started
    if delete_pdf and res.erreurs == 0 and (res.extraites + res.rendues + res.sautees) == res.pages:
        pdf_path.unlink(missing_ok=True)
    return res


def _job(payload: Tuple[str, str, int, bool, bool, bool]) -> NumeroResult:
    pdf, out, min_long, gray, force, delete_pdf = payload
    return convert_numero(
        Path(pdf), Path(out),
        min_long=min_long, gray=gray, force=force, delete_pdf=delete_pdf,
    )


def discover(pdf_root: Path, revues: Optional[List[str]]) -> List[Tuple[Path, str, str]]:
    """(<pdf>, <revue>, <numero_id>) pour chaque PDF sous `pdf_root`."""
    found = []
    for pdf in sorted(pdf_root.glob("*/*/*.pdf")):
        numero_id = pdf.parent.name
        revue = pdf.parent.parent.name
        if revues and revue not in revues:
            continue
        found.append((pdf, revue, numero_id))
    return found


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Etape 3 : PDF Gallica -> images de page, par extraction du "
                    "flux embarque (pypdfium2). Voir la docstring du module."
    )
    ap.add_argument("--pdf-root", default="pdf_process")
    ap.add_argument("--images-root", default="images_process")
    ap.add_argument("--revue", action="append", default=None,
                    help="limiter a cette revue (repetable).")
    ap.add_argument("--min-long", type=int, default=MIN_LONG_DEFAULT,
                    help=f"cote long minimal d'une image de page en px. Def {MIN_LONG_DEFAULT} "
                         "(le bandeau Gallica fait 642x251).")
    ap.add_argument("--gray", action="store_true",
                    help="convertir en 8-bit gris. COUTEUX : annule le benefice du "
                         "passthrough (decode + reencode). Le scan embarque est deja "
                         "ce que Gallica a numerise ; le passer en gris n'ajoute rien.")
    ap.add_argument("--jobs", type=int, default=1,
                    help="numeros traites en parallele (1 process par numero).")
    ap.add_argument("--force", action="store_true", help="re-ecrire les pages deja presentes.")
    ap.add_argument("--delete-pdf-after-success", action="store_true",
                    help="supprimer le PDF une fois toutes ses pages ecrites sans erreur.")
    ap.add_argument("--limit", type=int, default=None, help="(test) n premiers numeros.")
    ap.add_argument("--report", default=None, help="ecrire le rapport JSON ici.")
    args = ap.parse_args()

    pdf_root = Path(args.pdf_root)
    images_root = Path(args.images_root)
    if not pdf_root.is_dir():
        sys.exit(f"pdf-root introuvable : {pdf_root}")

    todo = discover(pdf_root, args.revue)
    if args.limit:
        todo = todo[: args.limit]
    if not todo:
        print("[INFO] Aucun PDF a convertir.")
        return

    print(f"[INFO] {len(todo)} numero(s) a traiter, {args.jobs} job(s) en parallele.")
    payloads = [
        (str(pdf), str(images_root / revue / numero_id),
         args.min_long, args.gray, args.force, args.delete_pdf_after_success)
        for pdf, revue, numero_id in todo
    ]

    results: List[NumeroResult] = []
    t0 = time.monotonic()
    if args.jobs <= 1:
        for payload in payloads:
            results.append(_job(payload))
            _log(results[-1], len(results), len(payloads))
    else:
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(_job, p): p for p in payloads}
            for fut in as_completed(futures):
                results.append(fut.result())
                _log(results[-1], len(results), len(payloads))

    elapsed = time.monotonic() - t0
    _summary(results, elapsed)
    if args.report:
        Path(args.report).write_text(
            json.dumps([r.__dict__ for r in results], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _log(r: NumeroResult, i: int, total: int) -> None:
    done = r.extraites + r.rendues
    spp = (r.secondes / done) if done else 0.0
    flag = f"  ERREURS={r.erreurs} {r.message}" if r.erreurs else ""
    rendu = f" rendues={r.rendues}" if r.rendues else ""
    print(f"[{i}/{total}] {r.revue}/{r.numero_id} : {r.pages} p "
          f"(extraites={r.extraites}{rendu} sautees={r.sautees}) "
          f"{r.octets/1e6:.0f} Mo  {r.secondes:.0f} s ({spp:.3f} s/page){flag}",
          flush=True)


def _summary(results: List[NumeroResult], elapsed: float) -> None:
    pages = sum(r.extraites + r.rendues for r in results)
    octets = sum(r.octets for r in results)
    erreurs = sum(r.erreurs for r in results)
    rendues = sum(r.rendues for r in results)
    raisons: Dict[str, int] = {}
    for r in results:
        for k, v in r.raisons_rendu.items():
            raisons[k] = raisons.get(k, 0) + v
    print("\n===== RESUME =====")
    print(f"numeros      : {len(results)}")
    print(f"pages ecrites: {pages}  ({sum(r.sautees for r in results)} deja presentes)")
    print(f"  extraites  : {sum(r.extraites for r in results)}")
    print(f"  rendues    : {rendues}" + (f"  {raisons}" if raisons else ""))
    # le poids couvre AUSSI les pages deja presentes, sinon une passe de reprise
    # (0 page ecrite) afficherait tout le poids ramene a une seule page.
    sur_disque = pages + sum(r.sautees for r in results)
    print(f"poids        : {octets/1e9:.2f} Go  ({octets/1e6/max(sur_disque,1):.2f} Mo/page)")
    # meme precaution que pour le poids : une passe de reprise n'ecrit rien, et
    # ramener sa duree a 1 page afficherait un "21 s/page" absurde.
    print(f"duree        : {elapsed:.0f} s"
          + (f"  ({elapsed/pages:.3f} s/page)" if pages else "  (rien a convertir)"))
    gabarits: Dict[str, int] = {}
    for r in results:
        if r.cote_long_median:
            bucket = f"{(r.cote_long_median // 500) * 500}-{(r.cote_long_median // 500) * 500 + 499}px"
            gabarits[bucket] = gabarits.get(bucket, 0) + 1
    if gabarits:
        print("cote long    : " + "  ".join(f"{k}:{v}" for k, v in sorted(gabarits.items())))
    if erreurs:
        print(f"ERREURS      : {erreurs}")


if __name__ == "__main__":
    main()
