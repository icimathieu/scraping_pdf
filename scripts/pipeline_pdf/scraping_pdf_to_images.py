import argparse
import csv
import json
import re
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pdf2image import convert_from_path, pdfinfo_from_path
from pdf2image.exceptions import PDFInfoNotInstalledError, PDFPageCountError, PDFSyntaxError

NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")


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


class CircuitBreakerFailures:
    def __init__(self, threshold: int, sleep_seconds: int) -> None:
        self.threshold = max(1, threshold)
        self.sleep_seconds = max(1, sleep_seconds)
        self.consecutive_failures = 0

    def record_success(self) -> None:
        if self.consecutive_failures > 0:
            print(
                f"[INFO][circuit_breaker] reset consecutive_failures={self.consecutive_failures} after success"
            )
        self.consecutive_failures = 0

    def record_failure(self, context: str) -> None:
        self.consecutive_failures += 1
        print(
            f"[WARN][circuit_breaker] failures streak={self.consecutive_failures}/{self.threshold} "
            f"context={context}"
        )
        if self.consecutive_failures >= self.threshold:
            print(
                f"[WARN][circuit_breaker] Sleeping {self.sleep_seconds}s after {self.consecutive_failures} consecutive failures"
            )
            time.sleep(self.sleep_seconds)
            self.consecutive_failures = 0


def sanitize_path_part(value: str, fallback: str) -> str:
    clean = NON_ALNUM_PATTERN.sub("_", value).strip("._")
    return clean if clean else fallback


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, PDFInfoNotInstalledError):
        return "pdfinfo_not_installed"
    if isinstance(exc, PDFPageCountError):
        return "pdf_page_count_error"
    if isinstance(exc, PDFSyntaxError):
        return "pdf_syntax_error"
    if isinstance(exc, ValueError):
        return "value_error"
    if isinstance(exc, OSError):
        return "os_error"
    return exc.__class__.__name__


def load_payload(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Le fichier d'entree doit contenir un objet JSON.")
    return payload


def ensure_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = payload.get("items")
    if isinstance(items, list):
        return items
    raise ValueError("Le fichier d'entree doit contenir une liste 'items'.")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def save_csv(path: Path, items: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "revue",
        "parent_ark_date",
        "year",
        "day_of_year",
        "numero_id",
        "issue_ark",
        "precision",
        "pdf_url",
        "pdf_path",
        "pdf_size_bytes",
        "pdf_deleted",
        "image_output_dir",
        "images_total",
        "images_converted",
        "images_existing",
        "images_errors",
        "status",
        "error_stage",
        "error_code",
        "error_message",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow({key: item.get(key, "") for key in fieldnames})


def resolve_pdf_path(item: Dict[str, Any], pdf_root: Path) -> Path:
    pdf_path_raw = str(item.get("pdf_path", "")).strip()
    if pdf_path_raw:
        return Path(pdf_path_raw)

    revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
    numero_id = sanitize_path_part(str(item.get("numero_id", "")), "numero")
    new_layout = pdf_root / revue / numero_id / f"{numero_id}.pdf"
    old_layout = pdf_root / numero_id / f"{numero_id}.pdf"
    return new_layout if new_layout.exists() else old_layout


def cleanup_empty_dirs(start_dir: Path, stop_dir: Path) -> None:
    current = start_dir
    try:
        stop_resolved = stop_dir.resolve()
    except Exception:
        stop_resolved = stop_dir
    while True:
        if not current.exists():
            break
        try:
            current_resolved = current.resolve()
        except Exception:
            current_resolved = current
        if current_resolved == stop_resolved:
            break
        try:
            current.rmdir()
        except OSError:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent


def convert_pdf_page_to_bitonal(
    pdf_path: Path,
    output_path: Path,
    page_number: int,
    dpi: int,
    bitonal_threshold: int,
    image_format: str,
    poppler_path: Optional[str],
) -> None:
    images = convert_from_path(
        pdf_path=str(pdf_path),
        dpi=dpi,
        first_page=page_number,
        last_page=page_number,
        fmt="ppm",
        thread_count=1,
        poppler_path=poppler_path,
    )
    if not images:
        raise ValueError("Aucune image retournee pendant la conversion du PDF")
    grayscale = images[0].convert("L")
    bitonal = grayscale.point(
        lambda px: 255 if px >= bitonal_threshold else 0,
        mode="1",
    )

    if image_format == "png":
        bitonal.save(output_path, format="PNG", optimize=True)
        return

    if image_format == "tiff":
        bitonal.save(output_path, format="TIFF", compression="group4")
        return

    raise ValueError(f"Format image non supporte: {image_format}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline PDF - Etape 3: conversion des PDF en images bitonales."
    )
    parser.add_argument("--input", default="input/arks_numeros.json")
    parser.add_argument("--output", default="input/arks_numeros.json")
    parser.add_argument("--output-csv", default="input/tableau_arks_numeros.csv")
    parser.add_argument("--pdf-root", default="pdf_process")
    parser.add_argument("--image-root", default="images_process")
    parser.add_argument("--requests-per-minute", type=int, default=120)
    parser.add_argument("--cb-threshold", type=int, default=5)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--bitonal-threshold", type=int, default=180)
    parser.add_argument("--image-format", choices=("png", "tiff"), default="tiff")
    parser.add_argument("--first-page", type=int, default=1)
    parser.add_argument("--last-page", type=int, default=0)
    parser.add_argument("--poppler-path", default="")
    parser.add_argument("--delete-pdf-after-success", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_csv_path = Path(args.output_csv)
    pdf_root = Path(args.pdf_root)
    image_root = Path(args.image_root)
    poppler_path = args.poppler_path.strip() or None
    if args.bitonal_threshold < 0 or args.bitonal_threshold > 255:
        raise ValueError("--bitonal-threshold doit etre compris entre 0 et 255.")
    if args.first_page < 1:
        raise ValueError("--first-page doit etre >= 1.")
    if args.last_page < 0:
        raise ValueError("--last-page doit etre >= 0 (0 = derniere page).")

    payload = load_payload(input_path)
    items = ensure_items(payload)
    limiter = RateLimiter(args.requests_per_minute)
    circuit_breaker = CircuitBreakerFailures(args.cb_threshold, args.cb_sleep_seconds)

    converted_ok = 0
    converted_error = 0
    skipped_prior_error = 0

    for item in items:
        if str(item.get("status", "")).strip() == "error" and not str(
            item.get("issue_ark", "")
        ).strip():
            print(
                f"[ERROR][step3][skip_prior_error][{item.get('revue','')}][{item.get('numero_id','')}] "
                f"{item.get('error_stage','')} {item.get('error_code','')} {item.get('error_message','')}"
            )
            skipped_prior_error += 1
            continue

        revue_raw = str(item.get("revue", "")).strip()
        numero_id_raw = str(item.get("numero_id", "")).strip()

        if not revue_raw or not numero_id_raw:
            item["status"] = "error"
            item["error_stage"] = "pdf_to_jpg_input_validation"
            item["error_code"] = "missing_required_fields"
            item["error_message"] = "revue ou numero_id manquant"
            item["images_total"] = 0
            item["images_converted"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            print(
                f"[ERROR][step3][pdf_to_jpg_input_validation][{revue_raw}][{numero_id_raw}] "
                f"{item['error_message']}"
            )
            converted_error += 1
            continue

        revue_safe = sanitize_path_part(revue_raw, "inconnue")
        numero_safe = sanitize_path_part(numero_id_raw, "numero")

        pdf_path = resolve_pdf_path(item, pdf_root)
        item["pdf_path"] = pdf_path.as_posix()
        item["pdf_deleted"] = False

        output_dir = image_root / revue_safe / numero_safe
        output_dir.mkdir(parents=True, exist_ok=True)
        item["image_output_dir"] = output_dir.as_posix()

        if not pdf_path.exists() or pdf_path.stat().st_size <= 0:
            item["status"] = "error"
            item["error_stage"] = "pdf_to_jpg_source"
            item["error_code"] = "pdf_not_found"
            item["error_message"] = f"PDF introuvable ou vide: {pdf_path}"
            item["images_total"] = 0
            item["images_converted"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            print(
                f"[ERROR][step3][pdf_to_jpg_source][{revue_raw}][{numero_id_raw}] "
                f"{item['error_code']} {item['error_message']}"
            )
            converted_error += 1
            continue

        try:
            pdf_info = pdfinfo_from_path(str(pdf_path), poppler_path=poppler_path)
            total_pages = int(pdf_info.get("Pages", 0))
            if total_pages <= 0:
                raise ValueError("Nombre de pages nul ou introuvable")
            circuit_breaker.record_success()
        except Exception as exc:
            circuit_breaker.record_failure(
                context=f"revue={revue_raw} numero_id={numero_id_raw} stage=pdfinfo"
            )
            item["status"] = "error"
            item["error_stage"] = "pdf_to_jpg_pdfinfo"
            item["error_code"] = error_code_from_exception(exc)
            item["error_message"] = str(exc)
            item["images_total"] = 0
            item["images_converted"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            print(
                f"[ERROR][step3][pdf_to_jpg_pdfinfo][{revue_raw}][{numero_id_raw}] "
                f"{item['error_code']} {item['error_message']}"
            )
            converted_error += 1
            continue

        converted = 0
        existing = 0
        errors = 0
        first_error: Optional[Tuple[str, str]] = None

        start_page = args.first_page
        end_page = args.last_page if args.last_page > 0 else total_pages
        end_page = min(end_page, total_pages)
        if start_page > end_page:
            item["status"] = "error"
            item["error_stage"] = "pdf_to_jpg_page_range"
            item["error_code"] = "invalid_page_range"
            item["error_message"] = (
                f"Plage invalide: first_page={start_page}, last_page={args.last_page}, total_pages={total_pages}"
            )
            item["images_total"] = 0
            item["images_converted"] = 0
            item["images_existing"] = 0
            item["images_errors"] = 0
            print(
                f"[ERROR][step3][pdf_to_jpg_page_range][{revue_raw}][{numero_id_raw}] "
                f"{item['error_code']} {item['error_message']}"
            )
            converted_error += 1
            continue

        image_ext = "png" if args.image_format == "png" else "tif"

        for page_index in range(start_page, end_page + 1):
            image_path = output_dir / f"page_{page_index:04d}.{image_ext}"
            if image_path.exists() and image_path.stat().st_size > 0 and not args.force:
                existing += 1
                continue

            limiter.wait_turn()
            try:
                convert_pdf_page_to_bitonal(
                    pdf_path=pdf_path,
                    output_path=image_path,
                    page_number=page_index,
                    dpi=args.dpi,
                    bitonal_threshold=args.bitonal_threshold,
                    image_format=args.image_format,
                    poppler_path=poppler_path,
                )
                converted += 1
                circuit_breaker.record_success()
            except Exception as exc:
                errors += 1
                circuit_breaker.record_failure(
                    context=(
                        f"revue={revue_raw} numero_id={numero_id_raw} "
                        f"stage=convert page={page_index}"
                    )
                )
                if first_error is None:
                    first_error = (error_code_from_exception(exc), str(exc))

        selected_pages = (end_page - start_page + 1)
        item["images_total"] = selected_pages
        item["images_converted"] = converted
        item["images_existing"] = existing
        item["images_errors"] = errors

        if errors > 0:
            item["status"] = "error"
            item["error_stage"] = "pdf_to_jpg_convert"
            item["error_code"] = first_error[0] if first_error else "unknown_error"
            item["error_message"] = first_error[1] if first_error else "Erreur de conversion non detaillee"
            print(
                f"[ERROR][step3][pdf_to_jpg_convert][{revue_raw}][{numero_id_raw}] "
                f"{item['error_code']} {item['error_message']} (errors={errors}/{selected_pages})"
            )
            converted_error += 1
        else:
            item["status"] = "ok"
            item["error_stage"] = ""
            item["error_code"] = ""
            item["error_message"] = ""
            if args.delete_pdf_after_success:
                if pdf_path.exists():
                    try:
                        pdf_path.unlink()
                        cleanup_empty_dirs(pdf_path.parent, pdf_root)
                        item["pdf_deleted"] = True
                        item["pdf_size_bytes"] = 0
                        print(
                            f"[INFO][step3][pdf_cleanup][{revue_raw}][{numero_id_raw}] PDF supprime: {pdf_path}"
                        )
                    except Exception as exc:
                        item["pdf_deleted"] = False
                        print(
                            f"[WARN][step3][pdf_cleanup][{revue_raw}][{numero_id_raw}] "
                            f"Echec suppression PDF: {exc}"
                        )
            converted_ok += 1

    payload["total_issues"] = sum(1 for it in items if str(it.get("issue_ark", "")).strip())
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)
    payload["pdf_image_collection"] = {
        "requests_per_minute": args.requests_per_minute,
        "pdf_root": pdf_root.as_posix(),
        "image_root": image_root.as_posix(),
        "image_format": args.image_format,
        "dpi": args.dpi,
        "bitonal_threshold": args.bitonal_threshold,
        "delete_pdf_after_success": args.delete_pdf_after_success,
        "converted_ok": converted_ok,
        "converted_error": converted_error,
        "skipped_prior_error": skipped_prior_error,
    }

    save_json(output_path, payload)
    save_csv(output_csv_path, items)
    print(
        "Termine: "
        f"{converted_ok} numeros OK, {converted_error} numeros en erreur, "
        f"{skipped_prior_error} ignores (erreurs precedentes)"
    )
    print(f"JSON mis a jour: {output_path}")
    print(f"CSV mis a jour: {output_csv_path}")
    print(f"Images: {image_root}")


if __name__ == "__main__":
    main()
