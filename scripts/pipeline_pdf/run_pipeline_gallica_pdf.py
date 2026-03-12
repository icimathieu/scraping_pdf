import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def parse_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def resolve_path(path_value: str, root: Path) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else root / path


def stream_command(step_name: str, cmd: List[str], cwd: Path) -> int:
    print(f"[INFO][{step_name}] Running: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(f"[{step_name}] {line.rstrip()}")
    process.wait()
    return process.returncode


def recompute_payload_totals(payload: Dict[str, Any]) -> None:
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []
        payload["items"] = items
    payload["total_issues"] = sum(1 for it in items if str(it.get("issue_ark", "")).strip())
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)


def merge_step1_items(
    current_payload: Dict[str, Any],
    step1_payload: Dict[str, Any],
    processed_revues: List[str],
) -> Dict[str, Any]:
    current_items = current_payload.get("items", [])
    step1_items = step1_payload.get("items", [])
    if not isinstance(current_items, list):
        current_items = []
    if not isinstance(step1_items, list):
        step1_items = []

    filtered = [
        item for item in current_items if str(item.get("revue", "")) not in processed_revues
    ]
    merged_items = filtered + step1_items

    def sort_key(item: Dict[str, Any]) -> Tuple[str, int, int, str]:
        year = parse_int(item.get("year"), default=0)
        day = parse_int(item.get("day_of_year"), default=0)
        return (
            str(item.get("revue", "")),
            year,
            day,
            str(item.get("numero_id", "")),
        )

    merged_items.sort(key=sort_key)
    current_payload["items"] = merged_items
    if "period" in step1_payload:
        current_payload["period"] = step1_payload["period"]
    recompute_payload_totals(current_payload)
    return current_payload


def pdf_work_remaining(items: List[Dict[str, Any]]) -> int:
    def has_pdf_signature(path: Path) -> bool:
        try:
            if not path.exists() or path.stat().st_size <= 0:
                return False
            with path.open("rb") as fh:
                prefix = fh.read(64)
            candidate = prefix.lstrip()
            if candidate.startswith(b"\xef\xbb\xbf"):
                candidate = candidate[3:]
            return candidate.startswith(b"%PDF")
        except OSError:
            return False

    remaining = 0
    for item in items:
        if str(item.get("pipeline_status", "")).strip() == "done":
            continue
        issue_ark = str(item.get("issue_ark", "")).strip()
        if not issue_ark:
            continue
        pdf_path = str(item.get("pdf_path", "")).strip()
        if not pdf_path:
            remaining += 1
            continue
        p = Path(pdf_path)
        if not has_pdf_signature(p):
            remaining += 1
    return remaining


def images_work_remaining(items: List[Dict[str, Any]]) -> int:
    remaining = 0
    for item in items:
        if str(item.get("pipeline_status", "")).strip() == "done":
            continue
        issue_ark = str(item.get("issue_ark", "")).strip()
        if not issue_ark:
            continue
        total = parse_int(item.get("images_total"), default=0)
        converted = parse_int(item.get("images_converted"), default=0)
        existing = parse_int(item.get("images_existing"), default=0)
        errors = parse_int(item.get("images_errors"), default=0)
        done = total > 0 and errors == 0 and (converted + existing) >= total
        if not done:
            remaining += 1
    return remaining


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orchestrateur pipeline PDF Gallica (3 etapes)."
    )
    parser.add_argument("--revues-input", default="input/arks_revues.json")
    parser.add_argument("--numeros-json", default="input/arks_numeros.json")
    parser.add_argument("--numeros-csv", default="input/tableau_arks_numeros.csv")
    parser.add_argument("--pdf-root", default="pdf_process")
    parser.add_argument("--image-root", default="images_process")
    parser.add_argument("--state-file", default="manifest_iiif_process/state_pdf.json")
    parser.add_argument("--start-year", type=int, default=1870)
    parser.add_argument("--end-year", type=int, default=1914)
    parser.add_argument("--issues-rpm", type=int, default=10)
    parser.add_argument("--pdf-rpm", type=int, default=1)
    parser.add_argument("--image-rpm", type=int, default=120)
    parser.add_argument("--step1-cb-threshold", type=int, default=5)
    parser.add_argument("--step1-cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--step2-cb-threshold", type=int, default=5)
    parser.add_argument("--step2-cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--step3-cb-threshold", type=int, default=5)
    parser.add_argument("--step3-cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--timeout-pdf", type=int, default=30)
    parser.add_argument("--step2-progress-log-seconds", type=int, default=10)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--bitonal-threshold", type=int, default=180)
    parser.add_argument("--image-format", choices=("png", "tiff"), default="png")
    parser.add_argument("--poppler-path", default="")
    parser.add_argument("--delete-pdf-after-success", action="store_true")
    parser.add_argument("--step2-cookies-file", default="gallica.bnf.fr_cookies.txt")
    parser.add_argument("--step2-fail-fast-altcha", action="store_true")
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 14.0; rv:136.0) Gecko/20100101 Firefox/136.0",
    )
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--force-step1", action="store_true")
    parser.add_argument("--force-pdf", action="store_true")
    parser.add_argument("--force-images", action="store_true")
    parser.add_argument("--disable-step1", action="store_true")
    parser.add_argument("--disable-step2", action="store_true")
    parser.add_argument("--disable-step3", action="store_true")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent
    cwd = project_root

    revues_input = resolve_path(args.revues_input, project_root)
    numeros_json = resolve_path(args.numeros_json, project_root)
    numeros_csv = resolve_path(args.numeros_csv, project_root)
    pdf_root = resolve_path(args.pdf_root, project_root)
    image_root = resolve_path(args.image_root, project_root)
    state_file = resolve_path(args.state_file, project_root)

    step1_script = script_dir / "scraping_arks_numeros_gallica_pdf.py"
    step2_script = script_dir / "scraping_pdf.py"
    step3_script = script_dir / "scraping_pdf_to_images.py"

    tmp_dir = state_file.parent / "_tmp_pdf"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    revues = load_json(revues_input, default={})
    if not isinstance(revues, dict):
        raise ValueError("Le fichier revues doit contenir un objet JSON {revue: ark/url}.")

    state = load_json(state_file, default={"revues": {}, "runs": []})
    if not isinstance(state, dict):
        state = {"revues": {}, "runs": []}
    state.setdefault("revues", {})
    state.setdefault("runs", [])

    run_meta = {
        "started_at": now_iso(),
        "status": "running",
        "step1_rc": None,
        "step2_rc": None,
        "step3_rc": None,
    }
    state["runs"].append(run_meta)
    save_json(state_file, state)

    if args.disable_step1:
        print("[INFO][step1] Disabled by --disable-step1")
    else:
        pending_revues: Dict[str, str] = {}
        for revue_name, ark_value in revues.items():
            ark_str = str(ark_value)
            revue_state = state["revues"].get(revue_name, {})
            needs_refresh = (
                args.force_step1
                or revue_state.get("status") != "done"
                or revue_state.get("ark") != ark_str
            )
            if needs_refresh:
                pending_revues[revue_name] = ark_str

        if not pending_revues:
            print("[INFO][step1] Aucun nouveau titre a traiter.")
        else:
            print(f"[INFO][step1] Revues a traiter: {len(pending_revues)}")
            tmp_step1_input = tmp_dir / "step1_pending_revues.json"
            tmp_step1_json = tmp_dir / "step1_output.json"
            tmp_step1_csv = tmp_dir / "step1_output.csv"
            save_json(tmp_step1_input, pending_revues)

            step1_cmd = [
                args.python_bin,
                str(step1_script),
                "--input",
                str(tmp_step1_input),
                "--output-json",
                str(tmp_step1_json),
                "--output-csv",
                str(tmp_step1_csv),
                "--start-year",
                str(args.start_year),
                "--end-year",
                str(args.end_year),
                "--requests-per-minute",
                str(args.issues_rpm),
                "--cb-threshold",
                str(args.step1_cb_threshold),
                "--cb-sleep-seconds",
                str(args.step1_cb_sleep_seconds),
                "--user-agent",
                args.user_agent,
            ]
            rc = stream_command("step1", step1_cmd, cwd)
            run_meta["step1_rc"] = rc

            step1_payload = load_json(tmp_step1_json, default={"items": []})
            current_payload = load_json(numeros_json, default={"items": []})
            if not isinstance(current_payload, dict):
                current_payload = {"items": []}
            merged = merge_step1_items(
                current_payload=current_payload,
                step1_payload=step1_payload,
                processed_revues=list(pending_revues.keys()),
            )
            save_json(numeros_json, merged)

            step1_items = step1_payload.get("items", [])
            for revue_name, ark_value in pending_revues.items():
                revue_rows = [
                    it for it in step1_items if str(it.get("revue", "")) == revue_name
                ]
                revue_errors = [
                    it for it in revue_rows if str(it.get("status", "")).strip() == "error"
                ]
                if revue_errors:
                    first = revue_errors[0]
                    state["revues"][revue_name] = {
                        "status": "error",
                        "ark": ark_value,
                        "updated_at": now_iso(),
                        "last_error": (
                            f"{first.get('error_stage', '')} "
                            f"{first.get('error_code', '')} "
                            f"{first.get('error_message', '')}"
                        ).strip(),
                    }
                else:
                    state["revues"][revue_name] = {
                        "status": "done",
                        "ark": ark_value,
                        "updated_at": now_iso(),
                        "last_error": "",
                    }
            save_json(state_file, state)
            if rc != 0:
                print(
                    "[ERROR][step1] Le script s'est termine avec erreur. "
                    "Pipeline continue avec les donnees disponibles."
                )

    payload_after_step1 = load_json(numeros_json, default={"items": []})
    if not isinstance(payload_after_step1, dict):
        payload_after_step1 = {"items": []}
    items = payload_after_step1.get("items", [])
    if not isinstance(items, list):
        items = []

    if args.disable_step2:
        print("[INFO][step2] Disabled by --disable-step2")
    else:
        pending_pdf = pdf_work_remaining(items)
        if pending_pdf == 0 and not args.force_pdf:
            print("[INFO][step2] Tous les PDF semblent deja presents. Skip.")
            run_meta["step2_rc"] = 0
        else:
            print(f"[INFO][step2] PDF restants a traiter: {pending_pdf}")
            step2_cmd = [
                args.python_bin,
                str(step2_script),
                "--input",
                str(numeros_json),
                "--output",
                str(numeros_json),
                "--output-csv",
                str(numeros_csv),
                "--pdf-root",
                str(pdf_root),
                "--requests-per-minute",
                str(args.pdf_rpm),
                "--cb-threshold",
                str(args.step2_cb_threshold),
                "--cb-sleep-seconds",
                str(args.step2_cb_sleep_seconds),
                "--timeout-seconds",
                str(args.timeout_pdf),
                "--progress-log-seconds",
                str(args.step2_progress_log_seconds),
                "--cookies-file",
                str(args.step2_cookies_file),
                "--user-agent",
                args.user_agent,
            ]
            if args.step2_fail_fast_altcha:
                step2_cmd.append("--fail-fast-altcha")
            if args.force_pdf:
                step2_cmd.append("--force")
            rc = stream_command("step2", step2_cmd, cwd)
            run_meta["step2_rc"] = rc
            if rc != 0:
                print("[ERROR][step2] Le script s'est termine avec erreur.")

    payload_after_step2 = load_json(numeros_json, default={"items": []})
    if not isinstance(payload_after_step2, dict):
        payload_after_step2 = {"items": []}
    items_after_step2 = payload_after_step2.get("items", [])
    if not isinstance(items_after_step2, list):
        items_after_step2 = []

    if args.disable_step3:
        print("[INFO][step3] Disabled by --disable-step3")
    else:
        pending_images = images_work_remaining(items_after_step2)
        if pending_images == 0 and not args.force_images:
            print("[INFO][step3] Tous les numeros semblent deja convertis en JPG. Skip.")
            run_meta["step3_rc"] = 0
        else:
            print(f"[INFO][step3] Numeros restants a convertir: {pending_images}")
            step3_cmd = [
                args.python_bin,
                str(step3_script),
                "--input",
                str(numeros_json),
                "--output",
                str(numeros_json),
                "--output-csv",
                str(numeros_csv),
                "--pdf-root",
                str(pdf_root),
                "--image-root",
                str(image_root),
                "--requests-per-minute",
                str(args.image_rpm),
                "--cb-threshold",
                str(args.step3_cb_threshold),
                "--cb-sleep-seconds",
                str(args.step3_cb_sleep_seconds),
                "--dpi",
                str(args.dpi),
                "--bitonal-threshold",
                str(args.bitonal_threshold),
                "--image-format",
                args.image_format,
            ]
            if args.poppler_path.strip():
                step3_cmd.extend(["--poppler-path", args.poppler_path.strip()])
            if args.delete_pdf_after_success:
                step3_cmd.append("--delete-pdf-after-success")
            if args.force_images:
                step3_cmd.append("--force")
            rc = stream_command("step3", step3_cmd, cwd)
            run_meta["step3_rc"] = rc
            if rc != 0:
                print("[ERROR][step3] Le script s'est termine avec erreur.")

    status = "done"
    for key in ("step1_rc", "step2_rc", "step3_rc"):
        rc = run_meta.get(key)
        if rc is not None and rc != 0:
            status = "error"
            break
    run_meta["status"] = status
    run_meta["finished_at"] = now_iso()
    state["last_run"] = run_meta["finished_at"]
    save_json(state_file, state)

    print(f"[INFO][pipeline_pdf] Termine avec statut: {status}")
    print(f"[INFO][pipeline_pdf] State: {state_file}")
    print(f"[INFO][pipeline_pdf] JSON: {numeros_json}")
    print(f"[INFO][pipeline_pdf] CSV: {numeros_csv}")


if __name__ == "__main__":
    main()
