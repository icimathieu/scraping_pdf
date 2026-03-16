import argparse
import sys
import time
from pathlib import Path
from typing import Iterable, Set
from urllib.parse import urlparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service


def build_firefox_options(download_dir: Path, headless: bool, firefox_binary: Path) -> Options:
    options = Options()
    if headless:
        options.add_argument("-headless")
    options.binary_location = str(firefox_binary)

    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", str(download_dir))
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        ",".join(
            [
                "application/pdf",
                "application/octet-stream",
                "application/x-pdf",
                "binary/octet-stream",
            ]
        ),
    )
    options.set_preference("pdfjs.disabled", True)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.alwaysOpenPanel", False)
    return options


def list_download_candidates(download_dir: Path) -> Set[Path]:
    return {
        path
        for path in download_dir.iterdir()
        if path.is_file() and not path.name.endswith(".part")
    }


def wait_for_download(
    download_dir: Path,
    existing_files: Iterable[Path],
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> Path:
    existing_set = set(existing_files)
    deadline = time.monotonic() + timeout_seconds
    observed_candidate = None
    observed_size = None
    stable_count = 0

    while time.monotonic() < deadline:
        part_files = [path for path in download_dir.glob("*.part") if path.is_file()]
        current_files = list_download_candidates(download_dir)
        new_files = [path for path in current_files if path not in existing_set]

        if new_files and not part_files:
            candidate = max(new_files, key=lambda path: path.stat().st_mtime)
            size = candidate.stat().st_size
            if candidate == observed_candidate and size == observed_size and size > 0:
                stable_count += 1
            else:
                observed_candidate = candidate
                observed_size = size
                stable_count = 1

            if stable_count >= 2:
                return candidate

        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Telechargement non detecte dans {download_dir} avant timeout={timeout_seconds}s"
    )


def infer_filename_from_url(pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    name = Path(parsed.path).name
    return name or "download.pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Telecharge un PDF via Firefox/Selenium a partir d'une URL."
    )
    parser.add_argument("--pdf-url", required=True, help="URL directe ou redirigee vers un PDF.")
    parser.add_argument(
        "--download-dir",
        default="pdf_process/selenium_downloads",
        help="Dossier de destination du PDF telecharge.",
    )
    parser.add_argument(
        "--geckodriver-path",
        default="/opt/homebrew/bin/geckodriver",
        help="Chemin vers geckodriver.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="Temps maximal d'attente du telechargement.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=0.5,
        help="Intervalle de polling pour detecter la fin du telechargement.",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Lance Firefox en mode headless.",
    )
    parser.add_argument(
        "--firefox-binary-path",
        default="/Applications/Firefox.app/Contents/MacOS/firefox",
        help="Chemin vers le binaire Firefox.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    download_dir = Path(args.download_dir).expanduser().resolve()
    geckodriver_path = Path(args.geckodriver_path).expanduser().resolve()
    firefox_binary = Path(args.firefox_binary_path).expanduser().resolve()

    if args.timeout_seconds < 1:
        raise ValueError("--timeout-seconds doit etre >= 1.")
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds doit etre > 0.")
    if not geckodriver_path.exists():
        raise FileNotFoundError(f"geckodriver introuvable: {geckodriver_path}")
    if not firefox_binary.exists():
        raise FileNotFoundError(f"Firefox introuvable: {firefox_binary}")

    download_dir.mkdir(parents=True, exist_ok=True)
    existing_files = list_download_candidates(download_dir)

    options = build_firefox_options(download_dir, args.headless, firefox_binary)
    service = Service(str(geckodriver_path))
    driver = webdriver.Firefox(service=service, options=options)

    try:
        driver.set_page_load_timeout(5)
        print(f"[INFO] URL demandee: {args.pdf_url}", flush=True)
        print(f"[INFO] Dossier de telechargement: {download_dir}", flush=True)
        print(f"[INFO] Nom attendu approximatif: {infer_filename_from_url(args.pdf_url)}", flush=True)
        driver.get("about:blank")
        try:
            driver.execute_script("window.location.href = arguments[0];", args.pdf_url)
        except TimeoutException:
            # Un telechargement PDF n'aboutit pas toujours a un chargement de page navigable.
            pass
        downloaded_file = wait_for_download(
            download_dir=download_dir,
            existing_files=existing_files,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        print(f"[OK] PDF telecharge: {downloaded_file}", flush=True)
        return 0
    finally:
        driver.quit()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
