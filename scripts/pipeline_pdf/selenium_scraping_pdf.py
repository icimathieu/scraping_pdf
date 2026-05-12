import argparse
import csv
import http.cookiejar
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    InvalidCookieDomainException,
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")
ARK_PATTERN = re.compile(r"(ark:/12148/[a-z0-9]+)", re.IGNORECASE)


class CaptchaRequiredError(Exception):
    """Raised when Gallica serves ALTCHA instead of the requested PDF."""


class TooManyRequestsError(Exception):
    """Raised when Gallica serves a 429/rate-limit page."""


class ThrottleStopError(RuntimeError):
    """Raised when repeated throttling indicates the script should stop entirely."""


class RateLimiter:
    def __init__(self, requests_per_minute: float) -> None:
        self.requests_per_minute = max(0.01, float(requests_per_minute))
        self.min_interval_seconds = 60.0 / self.requests_per_minute
        self.next_allowed_at = 0.0

    def wait_turn(self) -> None:
        now = time.monotonic()
        if now < self.next_allowed_at:
            sleep_for = self.next_allowed_at - now
            log(
                f"[INFO][rate_limiter] Sleeping {sleep_for:.1f}s "
                f"(rpm={self.requests_per_minute:.3f})"
            )
            time.sleep(sleep_for)
        self.next_allowed_at = time.monotonic() + self.min_interval_seconds


class CircuitBreaker429:
    """Compte TOUTES les erreurs consecutives de telechargement.

    Toute erreur (429, ALTCHA, timeout, WebDriverException, fichier vide,
    signature PDF invalide, OSError, etc.) incremente le streak. Une reussite
    le remet a zero.
    """

    def __init__(self, threshold: int, sleep_seconds: int, max_cooldowns: int) -> None:
        self.threshold = max(1, threshold)
        self.sleep_seconds = max(1, sleep_seconds)
        self.max_cooldowns = max(1, max_cooldowns)
        self.consecutive_failures = 0
        self.cooldowns_used = 0

    def record_success(self) -> None:
        if self.consecutive_failures > 0:
            log(
                f"[INFO][circuit_breaker] reset consecutive_failures={self.consecutive_failures} after success"
            )
        self.consecutive_failures = 0

    def record_failure(self, exc: Exception, context: str) -> None:
        # On compte TOUTES les erreurs, pas seulement 429/ALTCHA :
        # un timeout repete, un Firefox qui crash, un fichier vide rendu sont
        # autant de signaux qu'il faut faire une pause.
        self.consecutive_failures += 1
        kind = exc.__class__.__name__
        log(
            f"[WARN][circuit_breaker] failures streak={self.consecutive_failures}/{self.threshold} "
            f"kind={kind} context={context}"
        )
        if self.consecutive_failures >= self.threshold:
            if self.cooldowns_used >= self.max_cooldowns:
                raise ThrottleStopError(
                    f"circuit_breaker_stop: {self.consecutive_failures} echecs consecutifs "
                    f"malgre {self.cooldowns_used} cooldown(s); arret definitif."
                )
            log(
                f"[WARN][circuit_breaker] Sleeping {self.sleep_seconds}s after "
                f"{self.consecutive_failures} consecutive failures "
                f"(cooldown {self.cooldowns_used + 1}/{self.max_cooldowns})"
            )
            time.sleep(self.sleep_seconds)
            self.cooldowns_used += 1
            self.consecutive_failures = 0


def log(message: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def sanitize_path_part(value: str, fallback: str) -> str:
    clean = NON_ALNUM_PATTERN.sub("_", value).strip("._")
    return clean if clean else fallback


def normalize_issue_ark(value: str) -> str:
    match = ARK_PATTERN.search(value)
    if not match:
        raise ValueError(f"ARK introuvable pour le numero: {value}")
    return match.group(1)


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, CaptchaRequiredError):
        return "captcha_required"
    if isinstance(exc, TooManyRequestsError):
        return "429"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, TimeoutException):
        return "timeout"
    if isinstance(exc, InvalidCookieDomainException):
        return "invalid_cookie_domain"
    if isinstance(exc, WebDriverException):
        return "webdriver_error"
    if isinstance(exc, FileNotFoundError):
        return "file_not_found"
    if isinstance(exc, OSError):
        return "os_error"
    if isinstance(exc, ValueError):
        return "value_error"
    return exc.__class__.__name__


def is_pdf_signature(content_prefix: bytes) -> bool:
    candidate = content_prefix.lstrip()
    if candidate.startswith(b"\xef\xbb\xbf"):
        candidate = candidate[3:]
    return candidate.startswith(b"%PDF")


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
        "status",
        "pipeline_status",
        "error_stage",
        "error_code",
        "error_message",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow({key: item.get(key, "") for key in fieldnames})


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


def build_firefox_options(
    download_dir: Path,
    headless: bool,
    firefox_binary: Path,
    user_agent: str,
) -> Options:
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
    if user_agent.strip():
        options.set_preference("general.useragent.override", user_agent.strip())
    return options


def load_cookies(cookies_file: str) -> List[http.cookiejar.Cookie]:
    if not cookies_file:
        return []
    cookie_path = Path(cookies_file)
    if not cookie_path.exists():
        log(f"[WARN][pdf] Fichier cookies introuvable: {cookie_path}")
        return []
    jar = http.cookiejar.MozillaCookieJar()
    jar.load(str(cookie_path), ignore_discard=True, ignore_expires=True)
    cookies = list(jar)

    # Check d'expiration : alerte si altcha_pass est expire ou expirera bientot.
    # Le token altcha_pass est la preuve qu'on a deja resolu un defi anti-bot
    # cote serveur ; il a typiquement une duree de vie de quelques heures.
    now_ts = time.time()
    altcha = next((c for c in cookies if c.name == "altcha_pass"), None)
    if altcha is None:
        log("[WARN][pdf][cookies] Aucun token 'altcha_pass' dans le cookies-file. "
            "Risque eleve de blocage ALTCHA — regenerer le fichier en resolvant un defi sur Gallica.")
    elif altcha.expires and altcha.expires < now_ts:
        from datetime import datetime, timezone
        expired_at = datetime.fromtimestamp(altcha.expires, tz=timezone.utc).isoformat()
        log(f"[WARN][pdf][cookies] Token altcha_pass EXPIRE depuis {expired_at}. "
            "Regenere le cookies-file via Firefox + extension cookies.txt.")
    elif altcha.expires:
        from datetime import datetime, timezone
        valid_until = datetime.fromtimestamp(altcha.expires, tz=timezone.utc).isoformat()
        remaining_h = (altcha.expires - now_ts) / 3600.0
        log(f"[INFO][pdf][cookies] Token altcha_pass valide jusqu'a {valid_until} "
            f"(reste {remaining_h:.1f}h).")

    return cookies


def seed_driver_cookies(
    driver: webdriver.Firefox,
    base_url: str,
    cookies_file: str,
) -> int:
    cookies = load_cookies(cookies_file)
    if not cookies:
        return 0
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"URL invalide pour l'initialisation des cookies: {base_url}")
    origin = f"{parsed.scheme}://{parsed.netloc}/"
    driver.get(origin)
    added = 0
    for cookie in cookies:
        domain = str(cookie.domain or "").lstrip(".")
        if domain and domain not in parsed.netloc:
            continue
        selenium_cookie = {
            "name": cookie.name,
            "value": cookie.value,
            "path": cookie.path or "/",
        }
        if cookie.domain:
            selenium_cookie["domain"] = cookie.domain
        if cookie.secure:
            selenium_cookie["secure"] = True
        if cookie.expires:
            selenium_cookie["expiry"] = int(cookie.expires)
        try:
            driver.add_cookie(selenium_cookie)
            added += 1
        except InvalidCookieDomainException:
            continue
    log(f"[INFO][pdf] Cookies charges dans Firefox: {added}")
    driver.get("about:blank")
    return added


def resolve_click_locator(args: argparse.Namespace) -> Optional[Tuple[str, str]]:
    if args.click_css_selector:
        return (By.CSS_SELECTOR, args.click_css_selector)
    if args.click_id_prefix:
        return (By.CSS_SELECTOR, f"[id^='{args.click_id_prefix}']")
    if args.click_xpath:
        return (By.XPATH, args.click_xpath)
    return None


def click_download_trigger(
    driver: webdriver.Firefox,
    locator: Tuple[str, str],
    timeout_seconds: int,
) -> None:
    element = WebDriverWait(driver, timeout_seconds).until(EC.element_to_be_clickable(locator))
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
        element,
    )
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)


def list_download_candidates(download_dir: Path) -> Set[Path]:
    return {
        path
        for path in download_dir.iterdir()
        if path.is_file() and not path.name.endswith(".part")
    }


def cleanup_new_downloads(download_dir: Path, existing_files: Iterable[Path]) -> None:
    existing_set = set(existing_files)
    for path in download_dir.iterdir():
        if path.is_file() and path not in existing_set:
            try:
                size = path.stat().st_size
                if size > 0:
                    with path.open("rb") as fh:
                        prefix = fh.read(8192)
                    if is_pdf_signature(prefix):
                        log(
                            f"[INFO][pdf][cleanup] Conservation du PDF telecharge: {path.name}"
                        )
                        continue
            except OSError:
                pass
            try:
                path.unlink()
            except OSError:
                pass


def infer_filename_from_url(pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    name = Path(parsed.path).name
    return name or "download.pdf"


def inspect_download_payload(path: Path) -> Tuple[int, bytes]:
    with path.open("rb") as fh:
        prefix = fh.read(8192)
    return path.stat().st_size, prefix


def raise_for_non_pdf_payload(path: Path) -> None:
    size_bytes, prefix = inspect_download_payload(path)
    if size_bytes <= 0:
        raise ValueError("empty_payload")
    if is_pdf_signature(prefix):
        return
    lowered = prefix.lstrip().lower()
    text = lowered.decode("utf-8", errors="ignore")
    if "altcha" in text:
        raise CaptchaRequiredError("captcha_required:payload_altcha")
    if "too many requests" in text or " 429" in text or text.startswith("429"):
        raise TooManyRequestsError("429:payload_page")
    if lowered.startswith(b"<!doctype html") or lowered.startswith(b"<html"):
        raise ValueError("non_pdf_html_payload")
    raise ValueError("non_pdf_signature")


def infer_failure_from_browser_state(driver: webdriver.Firefox) -> Exception:
    current_url = ""
    title = ""
    source = ""
    try:
        current_url = str(driver.current_url or "")
    except Exception:
        current_url = ""
    try:
        title = str(driver.title or "")
    except Exception:
        title = ""
    try:
        source = str(driver.page_source or "")
    except Exception:
        source = ""

    haystack = " ".join([current_url.lower(), title.lower(), source.lower()])
    if "altcha" in haystack:
        return CaptchaRequiredError(f"captcha_required:browser_state:{current_url or title}")
    if "429" in haystack or "too many requests" in haystack:
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")
    if source.strip():
        return ValueError(f"download_not_detected:url={current_url or 'unknown'}")
    return TimeoutError("telechargement_non_detecte")


def detect_429_from_browser_state(driver: webdriver.Firefox) -> Optional[Exception]:
    current_url = ""
    title = ""
    source = ""
    try:
        current_url = str(driver.current_url or "")
    except Exception:
        current_url = ""
    try:
        title = str(driver.title or "")
    except Exception:
        title = ""
    try:
        source = str(driver.page_source or "")
    except Exception:
        source = ""

    title_l = title.lower()
    source_l = source.lower()
    text_haystack = " ".join([title_l, source_l])

    explicit_markers = (
        "too many requests",
        "http 429",
        "http error 429",
        "status code 429",
        "error 429",
        "erreur 429",
    )
    if any(marker in text_haystack for marker in explicit_markers):
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")

    if re.search(r"\b429\b", text_haystack) and (
        "request" in text_haystack
        or "requete" in text_haystack
        or "rate limit" in text_haystack
        or "throttl" in text_haystack
    ):
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")
    return None


def wait_for_download(
    driver: webdriver.Firefox,
    download_dir: Path,
    existing_files: Iterable[Path],
    download_timeout_seconds: int,
    poll_interval_seconds: float,
    progress_log_seconds: int,
    stalled_timeout_seconds: int = 120,
) -> Path:
    """Attend qu'un PDF apparaisse dans download_dir.

    Strategie:
    - Tant que le .part grossit (le download progresse), on n'abandonne pas,
      meme au-dela du download_timeout_seconds. Logique : un PDF de 200 Mo
      sur une connexion lente peut depasser le timeout par defaut.
    - On abandonne uniquement si le .part est STABLE (taille inchangee)
      depuis stalled_timeout_seconds (defaut 120s = 2 min) : ca signifie
      que la connexion a ete dropee par le serveur.
    - Le download_timeout_seconds reste un plafond global tant que rien
      n'est detecte (ni .part, ni nouveau fichier).
    """
    existing_set = set(existing_files)
    deadline_global = time.monotonic() + download_timeout_seconds
    observed_candidate = None
    observed_size = None
    stable_count = 0
    last_progress_log = time.monotonic()

    # Suivi du progres .part pour detecter les downloads stalles vs actifs
    last_part_total_size = 0
    last_part_progress_at = time.monotonic()

    while True:
        now = time.monotonic()
        part_files = [path for path in download_dir.glob("*.part") if path.is_file()]
        current_files = list_download_candidates(download_dir)
        new_files = [path for path in current_files if path not in existing_set]

        # Taille totale des .part (nouveau)
        part_total_size = 0
        part_sizes_log = []
        for p in part_files:
            try:
                s = p.stat().st_size
                part_total_size += s
                part_sizes_log.append(f"{p.name}:{s}")
            except OSError:
                part_sizes_log.append(f"{p.name}:?")

        # Reset deadline si le .part progresse (= download actif)
        if part_total_size > last_part_total_size:
            last_part_total_size = part_total_size
            last_part_progress_at = now

        if progress_log_seconds > 0 and (now - last_progress_log) >= progress_log_seconds:
            new_sizes = []
            for path in sorted(new_files):
                try:
                    new_sizes.append(f"{path.name}:{path.stat().st_size}")
                except OSError:
                    new_sizes.append(f"{path.name}:?")
            stall_age = now - last_part_progress_at
            log(
                f"[INFO][pdf][download_wait] "
                f"part={part_sizes_log or ['none']} "
                f"new_files={new_sizes or ['none']} "
                f"stall_age={stall_age:.0f}s"
            )
            last_progress_log = now

        # Cas succes: un fichier final non-.part stable
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

        # Cas abandon: .part stalle depuis trop longtemps (= connexion dropee)
        if part_files and (now - last_part_progress_at) > stalled_timeout_seconds:
            raise TimeoutError(
                f"download_stalled: .part inchange depuis {now - last_part_progress_at:.0f}s "
                f"(seuil={stalled_timeout_seconds}s), connexion probablement coupee par le serveur"
            )

        # Cas patience: .part qui grossit -> on attend, meme au-dela du deadline global
        if part_files:
            time.sleep(poll_interval_seconds)
            continue

        # Pas de .part, pas de nouveau fichier -> on attend jusqu'au deadline global
        if now >= deadline_global:
            browser_exc = detect_429_from_browser_state(driver)
            if browser_exc is not None:
                raise browser_exc
            raise infer_failure_from_browser_state(driver)

        # Detection 429 / captcha si rien ne se passe
        if not new_files:
            browser_exc = detect_429_from_browser_state(driver)
            if browser_exc is not None:
                raise browser_exc

        time.sleep(poll_interval_seconds)


def trigger_download(
    driver: webdriver.Firefox,
    target_url: str,
    click_locator: Optional[Tuple[str, str]],
    click_timeout_seconds: int,
) -> None:
    # Toujours naviguer vers l'URL PDF. Un timeout de page n'est pas une erreur :
    # quand Gallica sert directement le PDF en attachement, la "page" ne finit
    # jamais de charger mais le telechargement demarre cote navigateur.
    try:
        driver.get(target_url)
    except TimeoutException:
        log(
            f"[INFO][pdf][page_load_timeout] navigation a depasse le timeout sur {target_url} "
            "(normal si le serveur stream le PDF directement), on continue."
        )

    if click_locator is None:
        return

    # Tentative de clic ALTCHA en BEST-EFFORT : si le checkbox est present,
    # on le clique pour debloquer le telechargement ; s'il est absent (cas le
    # plus frequent), on n'echoue pas, on laisse wait_for_download attendre
    # le fichier qui arrive deja via la navigation directe.
    log(
        f"[INFO][pdf][altcha_probe] locator={click_locator[1]} "
        f"timeout={click_timeout_seconds}s (best-effort)"
    )
    try:
        click_download_trigger(driver, click_locator, click_timeout_seconds)
        log("[INFO][pdf][altcha_clicked] checkbox ALTCHA clique, attente du telechargement")
    except (TimeoutException, NoSuchElementException, WebDriverException) as exc:
        log(
            f"[INFO][pdf][altcha_absent] pas de checkbox ALTCHA detecte "
            f"({exc.__class__.__name__}), passage en attente passive du telechargement"
        )


def file_has_pdf_signature(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        _, prefix = inspect_download_payload(path)
        return is_pdf_signature(prefix)
    except OSError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline PDF - Etape 2: telecharge les PDF via Firefox/Selenium."
    )
    parser.add_argument("--input", default="input/arks_numeros.json")
    parser.add_argument("--output", default="input/arks_numeros.json")
    parser.add_argument("--output-csv", default="input/tableau_arks_numeros.csv")
    parser.add_argument("--pdf-root", default="pdf_process")
    parser.add_argument("--requests-per-minute", type=float, default=0.3)
    parser.add_argument("--cb-threshold", type=int, default=3)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--cb-max-cooldowns", type=int, default=2)
    parser.add_argument("--page-timeout-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--stalled-timeout-seconds", type=int, default=120,
        help="Si le .part en cours de download n'a pas progresse depuis ce delai, "
             "on considere la connexion comme coupee et on abandonne ce PDF.")
    parser.add_argument(
        "--user-agent",
        default="",
        help="User-Agent a forcer dans Firefox. Vide (defaut) = laisse Firefox "
             "envoyer son User-Agent natif, ce qui evite les mismatch avec un "
             "altcha_pass genere depuis le meme Firefox.",
    )
    parser.add_argument("--cookies-file", default="gallica.bnf.fr_cookies.txt")
    parser.add_argument("--progress-log-seconds", type=int, default=10)
    parser.add_argument("--fail-fast-altcha", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    parser.add_argument("--click-timeout-seconds", type=int, default=15)
    parser.add_argument("--click-id-prefix", default="customAltcha_checkbox_")
    parser.add_argument("--click-css-selector", default="")
    parser.add_argument("--click-xpath", default="")
    parser.add_argument("--geckodriver-path", default="/opt/homebrew/bin/geckodriver")
    parser.add_argument(
        "--firefox-binary-path",
        default="/Applications/Firefox.app/Contents/MacOS/firefox",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Affiche Firefox au lieu du mode headless par defaut.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_csv_path = Path(args.output_csv)
    pdf_root = Path(args.pdf_root)
    geckodriver_path = Path(args.geckodriver_path).expanduser().resolve()
    firefox_binary = Path(args.firefox_binary_path).expanduser().resolve()
    download_dir = (pdf_root / "_selenium_downloads").resolve()
    headless = not args.show_browser

    if args.requests_per_minute <= 0:
        raise ValueError("--requests-per-minute doit etre > 0.")
    if args.cb_max_cooldowns < 1:
        raise ValueError("--cb-max-cooldowns doit etre >= 1.")
    if args.page_timeout_seconds < 1:
        raise ValueError("--page-timeout-seconds doit etre >= 1.")
    if args.timeout_seconds < 1:
        raise ValueError("--timeout-seconds doit etre >= 1.")
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds doit etre > 0.")
    if args.click_timeout_seconds < 1:
        raise ValueError("--click-timeout-seconds doit etre >= 1.")
    if not geckodriver_path.exists():
        raise FileNotFoundError(f"geckodriver introuvable: {geckodriver_path}")
    if not firefox_binary.exists():
        raise FileNotFoundError(f"Firefox introuvable: {firefox_binary}")

    payload = load_payload(input_path)
    items = ensure_items(payload)
    limiter = RateLimiter(args.requests_per_minute)
    circuit_breaker = CircuitBreaker429(
        args.cb_threshold,
        args.cb_sleep_seconds,
        args.cb_max_cooldowns,
    )
    total_items = len(items)
    with_issue_ark = sum(1 for it in items if str(it.get("issue_ark", "")).strip())
    log(
        f"[INFO][pdf] Demarrage step2 Selenium: total_items={total_items} "
        f"with_issue_ark={with_issue_ark} rpm={args.requests_per_minute:.3f} "
        f"page_timeout={args.page_timeout_seconds}s "
        f"download_timeout={args.timeout_seconds}s force={args.force} headless={headless}"
    )

    download_dir.mkdir(parents=True, exist_ok=True)
    options = build_firefox_options(download_dir, headless, firefox_binary, args.user_agent)
    service = Service(str(geckodriver_path))

    # Retry sur le demarrage Selenium : Firefox est parfois flaky au lancement
    # ("Process unexpectedly closed with status 0"). On retente jusqu'a 5 fois
    # avec un delai exponentiel.
    last_exc: Optional[Exception] = None
    driver = None
    for attempt in range(1, 6):
        try:
            driver = webdriver.Firefox(service=service, options=options)
            if attempt > 1:
                log(f"[INFO][pdf] Firefox demarre apres {attempt} tentative(s)")
            break
        except WebDriverException as exc:
            last_exc = exc
            wait = 5 * attempt
            log(
                f"[WARN][pdf] Echec demarrage Firefox (tentative {attempt}/5): {exc}. "
                f"Nouvel essai dans {wait}s."
            )
            time.sleep(wait)
    if driver is None:
        raise RuntimeError(
            f"Impossible de demarrer Firefox apres 5 tentatives: {last_exc}"
        )
    driver.set_page_load_timeout(max(args.page_timeout_seconds, 5))

    # Log de l'UA effectif (utile pour debugger les mismatch ALTCHA :
    # le UA doit correspondre a celui qui a genere le altcha_pass).
    try:
        driver.get("about:blank")
        actual_ua = driver.execute_script("return navigator.userAgent;")
        ua_source = "override (--user-agent)" if args.user_agent.strip() else "natif Firefox"
        log(f"[INFO][pdf] User-Agent effectif ({ua_source}): {actual_ua}")
    except Exception as exc:
        log(f"[WARN][pdf] Impossible de lire le User-Agent effectif: {exc}")
    if args.cookies_file:
        seed_driver_cookies(driver, "https://gallica.bnf.fr/", args.cookies_file)

    click_locator = resolve_click_locator(args)
    downloaded_count = 0
    existing_count = 0
    error_count = 0
    skipped_prior_error = 0
    fatal_stop = False

    try:
        for idx, item in enumerate(items, start=1):
            if str(item.get("status", "")).strip() == "error" and not str(
                item.get("issue_ark", "")
            ).strip():
                log(
                    f"[ERROR][pdf][skip_prior_error][{item.get('revue','')}][{item.get('numero_id','')}] "
                    f"{item.get('error_stage','')} {item.get('error_code','')} {item.get('error_message','')}"
                )
                skipped_prior_error += 1
                continue

            numero_id_raw = str(item.get("numero_id", "")).strip()
            issue_ark_raw = str(item.get("issue_ark", "")).strip()
            numero_id = sanitize_path_part(numero_id_raw, "numero")
            revue_raw = str(item.get("revue", ""))
            log(
                f"[INFO][pdf][progress] item={idx}/{total_items} revue={revue_raw} "
                f"numero={numero_id_raw or 'N/A'}"
            )

            if not numero_id_raw or not issue_ark_raw:
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_input_validation"
                item["error_code"] = "missing_required_fields"
                item["error_message"] = "numero_id ou issue_ark manquant"
                log(
                    f"[ERROR][pdf][pdf_input_validation][{item.get('revue','')}][{numero_id_raw}] "
                    f"{item['error_message']}"
                )
                error_count += 1
                continue

            try:
                issue_ark = normalize_issue_ark(issue_ark_raw)
            except Exception as exc:
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_ark_normalization"
                item["error_code"] = error_code_from_exception(exc)
                item["error_message"] = str(exc)
                log(
                    f"[ERROR][pdf][pdf_ark_normalization][{item.get('revue','')}][{numero_id}] "
                    f"{item['error_code']} {item['error_message']}"
                )
                error_count += 1
                continue

            pdf_url = f"https://gallica.bnf.fr/{issue_ark}.pdf"
            revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
            output_dir = pdf_root / revue / numero_id
            pdf_path = output_dir / f"{numero_id}.pdf"

            item["pdf_url"] = pdf_url
            item["pdf_path"] = pdf_path.as_posix()
            log(f"[INFO][pdf][url][{item.get('revue','')}][{numero_id}] {pdf_url}")

            if pdf_path.exists() and pdf_path.stat().st_size > 0 and not args.force:
                if file_has_pdf_signature(pdf_path):
                    item["pdf_size_bytes"] = pdf_path.stat().st_size
                    item["status"] = "ok"
                    item["pipeline_status"] = ""
                    item["error_stage"] = ""
                    item["error_code"] = ""
                    item["error_message"] = ""
                    log(
                        f"[INFO][pdf][skip_existing_valid][{item.get('revue','')}][{numero_id}] "
                        f"bytes={item['pdf_size_bytes']}"
                    )
                    existing_count += 1
                    continue
                log(
                    f"[WARN][pdf][existing_invalid_pdf][{item.get('revue','')}][{numero_id}] "
                    f"Fichier existant invalide, re-download: {pdf_path}"
                )
                try:
                    pdf_path.unlink()
                except OSError:
                    pass

            existing_files = list_download_candidates(download_dir)

            try:
                limiter.wait_turn()
                log(
                    f"[INFO][pdf][selenium_start][{item.get('revue','')}][{numero_id}] "
                    f"url={pdf_url} expected={infer_filename_from_url(pdf_url)}"
                )
                trigger_download(
                    driver=driver,
                    target_url=pdf_url,
                    click_locator=click_locator,
                    click_timeout_seconds=args.click_timeout_seconds,
                )
                downloaded_file = wait_for_download(
                    driver=driver,
                    download_dir=download_dir,
                    existing_files=existing_files,
                    download_timeout_seconds=args.timeout_seconds,
                    poll_interval_seconds=args.poll_interval_seconds,
                    progress_log_seconds=args.progress_log_seconds,
                    stalled_timeout_seconds=args.stalled_timeout_seconds,
                )
                raise_for_non_pdf_payload(downloaded_file)
                output_dir.mkdir(parents=True, exist_ok=True)
                try:
                    if pdf_path.exists():
                        pdf_path.unlink()
                except OSError:
                    pass
                downloaded_file.replace(pdf_path)
                item["pdf_size_bytes"] = pdf_path.stat().st_size
                item["status"] = "ok"
                item["pipeline_status"] = ""
                item["error_stage"] = ""
                item["error_code"] = ""
                item["error_message"] = ""
                circuit_breaker.record_success()
                log(
                    f"[INFO][pdf][downloaded][{item.get('revue','')}][{numero_id}] "
                    f"bytes={item['pdf_size_bytes']}"
                )
                downloaded_count += 1
            except Exception as exc:
                cleanup_new_downloads(download_dir, existing_files)
                cleanup_empty_dirs(output_dir, pdf_root)
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_download"
                item["error_code"] = error_code_from_exception(exc)
                item["error_message"] = str(exc)
                circuit_breaker.record_failure(exc, context=f"url={pdf_url}")
                log(
                    f"[ERROR][pdf][pdf_download][{item.get('revue','')}][{numero_id}] "
                    f"{item['error_code']} {item['error_message']}"
                )
                error_count += 1
                if args.fail_fast_altcha and item["error_code"] == "captcha_required":
                    fatal_stop = True
                    log(
                        f"[FATAL][pdf][captcha_required] ALTCHA detecte pour {pdf_url}. "
                        "Arret immediat de l'etape 2 (--fail-fast-altcha)."
                    )
                    break

            if idx % 25 == 0:
                log(
                    f"[INFO][pdf][checkpoint] processed={idx}/{total_items} "
                    f"downloaded={downloaded_count} existing={existing_count} "
                    f"errors={error_count} skipped={skipped_prior_error}"
                )
    finally:
        driver.quit()

    payload["total_issues"] = sum(
        1 for it in items if str(it.get("issue_ark", "")).strip()
    )
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)
    payload["pdf_collection"] = {
        "engine": "selenium",
        "requests_per_minute": args.requests_per_minute,
        "pdf_root": pdf_root.as_posix(),
        "download_dir": download_dir.as_posix(),
        "headless": headless,
        "downloaded": downloaded_count,
        "existing": existing_count,
        "errors": error_count,
        "skipped_prior_error": skipped_prior_error,
    }

    save_json(output_path, payload)
    save_csv(output_csv_path, items)
    log(
        f"Termine: {downloaded_count} PDF telecharges, {existing_count} deja presents, "
        f"{error_count} erreurs, {skipped_prior_error} ignores"
    )
    log(f"JSON mis a jour: {output_path}")
    log(f"CSV mis a jour: {output_csv_path}")
    log(f"PDF root: {pdf_root}")
    if fatal_stop:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
