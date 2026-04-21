# scraping_pdf — Gallica / IIIe République

Scraping des revues scientifiques numérisées sur Gallica (période 1870‑1914) pour produire des images bitonales destinées à l'océrisation (OCR via Tesseract, repo séparé).

## Structure du dossier

```
scraping_pdf/
├── input/
│   ├── arks_revues.json          # { nom_revue: ark_ou_url_perenne } — 12 revues cibles
│   ├── arks_numeros.json         # items[] par numéro (ARK, pdf_path, images, status...)
│   └── tableau_arks_numeros.csv  # miroir CSV du JSON ci-dessus
├── scripts/
│   ├── pipeline_pdf/             # pipeline active (PDF → images)
│   │   ├── run_pipeline_gallica_pdf.py       # orchestrateur (étapes 1+2+3)
│   │   ├── scraping_arks_numeros_gallica_pdf.py  # étape 1 : revues → ARK numéros
│   │   ├── selenium_scraping_pdf.py          # étape 2 : ARK numéro → PDF (Selenium/Firefox)
│   │   └── scraping_pdf_to_images.py         # étape 3 : PDF → PNG/TIFF bitonal
│   ├── pipeline_manifest_iiif/   # ancienne pipeline IIIF (abandonnée, cf. note)
│   └── scraping_pdfs_gallica.ipynb  # notebook exploratoire
├── pdf_process/                  # sorties étape 2 (ignoré par git) : <revue>/<numero_id>/<numero_id>.pdf
├── images_process/               # sorties étape 3 (ignoré par git) : <revue>/<numero_id>/page_XXXX.png
├── manifest_iiif_process/        # état orchestrateur (state_pdf.json) + legacy manifests
├── pipeline_pdf.md               # spécification langage naturel de la pipeline PDF (active)
├── pipeline_manifest.md          # spécification de l'ancienne pipeline IIIF (référence)
├── todo.md                       # état d'avancement par revue et par étape
├── readme.md                     # ce fichier
└── .venv/                        # environnement Python (requests, selenium, pdf2image, Pillow)
```

Les deux fichiers de spécification étape par étape sont à consulter en priorité :

- [pipeline_pdf.md](pipeline_pdf.md) — pipeline actuelle (PDF puis conversion locale), la seule utilisée aujourd'hui.
- [pipeline_manifest.md](pipeline_manifest.md) — pipeline IIIF historique (manifest → images full bitonal), gardée comme référence.

## Pipeline PDF — commandes

L'orchestrateur enchaîne les 3 étapes avec reprise/idempotence. La pipeline est **restartable** : chaque étape détecte ce qui reste à faire et saute ce qui est déjà fait.

### Lancer la pipeline complète (cas standard)

```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --revues-input input/arks_revues.json \
  --numeros-json input/arks_numeros.json \
  --numeros-csv input/tableau_arks_numeros.csv \
  --pdf-root pdf_process \
  --image-root images_process \
  --image-format png \
  --dpi 300 \
  --bitonal-threshold 180 \
  --delete-pdf-after-success
```

### Options principales de l'orchestrateur

Rate limiting / circuit breaker :
- `--issues-rpm 5` : requêtes/minute pour l'étape 1 (API Issues).
- `--pdf-rpm 0.5` : requêtes/minute pour l'étape 2 (téléchargement PDF Selenium).
- `--image-rpm 30` : throttle local conversion étape 3.
- `--step{1,2,3}-cb-threshold 5` : nb d'erreurs avant pause.
- `--step{1,2,3}-cb-sleep-seconds 600` : durée de pause après saturation.
- `--step{1,2,3}-cb-max-cooldowns 1` : nombre max de pauses avant abandon.

Période de collecte (étape 1) :
- `--start-year 1870` / `--end-year 1914`.

Conversion images (étape 3) :
- `--image-format png|tiff`, `--dpi 300`, `--bitonal-threshold 180`.
- `--delete-pdf-after-success` : supprime le PDF après conversion OK (économise de l'espace).

Sélection / forçage :
- `--disable-step1|2|3` : saute l'étape.
- `--force-step1|--force-pdf|--force-images` : refait tout, ignore l'idempotence.

Selenium (étape 2) :
- `--show-browser` : affiche Firefox (par défaut headless).
- `--step2-cookies-file chemin` : charge des cookies Firefox exportés (utile contre ALTCHA).
- `--step2-fail-fast-altcha` : stoppe si ALTCHA détecté.
- `--step2-page-timeout-seconds 60`, `--timeout-pdf 600`.

### Lancer une étape isolée

Étape 1 seule (récupérer ARK numéros pour une nouvelle revue) :
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step2 --disable-step3 --issues-rpm 5
```

Étape 2 seule (télécharger les PDFs restants) :
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step3 --pdf-rpm 0.5
```

Étape 3 seule (convertir les PDFs existants en images) :
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step2 --delete-pdf-after-success
```

### Appel direct des sous-scripts (debug)

Chaque script est autonome et accepte les mêmes familles d'options (cf. `--help` ou [pipeline_pdf.md](pipeline_pdf.md)) :

- Étape 1 : `scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py`
- Étape 2 : `scripts/pipeline_pdf/selenium_scraping_pdf.py` — nécessite Firefox + geckodriver (`/opt/homebrew/bin/geckodriver` par défaut).
- Étape 3 : `scripts/pipeline_pdf/scraping_pdf_to_images.py` — nécessite `poppler` (pdfinfo/pdftoppm).

## État de sortie

- `input/arks_numeros.json` est **enrichi à chaque étape** : chaque item gagne `pdf_path`, `pdf_size_bytes`, `images_total`, `images_converted`, `status`, `error_stage`, etc.
- `manifest_iiif_process/state_pdf.json` : état de l'orchestrateur (runs, revues done/error).
- Le CSV `tableau_arks_numeros.csv` est un miroir à plat pour inspection rapide.

Voir [todo.md](todo.md) pour l'état courant revue par revue.

## Prérequis

- Python 3 + `.venv` avec : `requests`, `selenium`, `pdf2image`, `Pillow`.
- Binaires système : Firefox, `geckodriver`, `poppler` (pour `pdfinfo`, `pdftoppm`).
- Respect des limites API Gallica (cf. https://api.bnf.fr/fr/node/232) : 5‑10 req/min max pour l'API Issues, très lent pour le PDF.
