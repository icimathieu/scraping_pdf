# scraping_pdf — Gallica / IIIe République

Scraping des revues scientifiques numérisées sur Gallica (période 1870‑1914) pour produire des images bitonales destinées à l'océrisation (OCR via Tesseract, repo séparé).

## Structure du dossier

```
scraping_pdf/
├── input/
│   ├── arks_revues.json          # { nom_revue: ark_ou_url_perenne } — 52 revues cibles
│   ├── arks_numeros.json         # items[] par numéro (ARK, pdf_path, images, status...)
│   ├── tableau_arks_numeros.csv  # miroir CSV du JSON ci-dessus
│   └── notices_revues.csv        # métadonnées bibliographiques (1 ligne par revue, pour le mémoire)
├── scripts/
│   ├── pipeline_pdf/             # pipeline active (PDF → images)
│   │   ├── run_pipeline_gallica_pdf.py       # orchestrateur (étapes 1+2+3, lance caffeinate)
│   │   ├── scraping_arks_numeros_gallica_pdf.py  # étape 1 : revues → ARK numéros
│   │   ├── selenium_scraping_pdf.py          # étape 2 : ARK numéro → PDF (Selenium/Firefox)
│   │   └── scraping_pdf_to_images.py         # étape 3 : PDF → PNG/TIFF bitonal
│   ├── pipeline_manifest_iiif/   # ancienne pipeline IIIF (abandonnée)
│   ├── scraping_notices_revues.py   # extraction métadonnées notices Gallica → CSV
│   └── scraping_pdfs_gallica.ipynb  # notebook exploratoire
├── pdf_process/                  # sorties étape 2 (gitignore) : <revue>/<numero_id>/<numero_id>.pdf
├── images_process/               # sorties étape 3 (gitignore) : <revue>/<numero_id>/page_XXXX.png
├── manifest_iiif_process/        # état orchestrateur (state_pdf.json) + legacy manifests
├── pipeline_pdf.md               # spécification langage naturel de la pipeline PDF (active)
├── pipeline_manifest.md          # spécification de l'ancienne pipeline IIIF (référence)
├── todo.md                       # état d'avancement par revue et par étape
├── readme.md                     # ce fichier
├── CLAUDE.md                     # conventions de commande (à lire avant CLI)
└── .venv/                        # environnement Python (requests, selenium, pdf2image, Pillow)
```

Les deux fichiers de spécification étape par étape sont à consulter en priorité :

- [pipeline_pdf.md](pipeline_pdf.md) — pipeline actuelle (PDF puis conversion locale), la seule utilisée aujourd'hui.
- [pipeline_manifest.md](pipeline_manifest.md) — pipeline IIIF historique (manifest → images full bitonal), gardée comme référence.

## Pipeline PDF — commandes

L'orchestrateur enchaîne les 3 étapes avec reprise/idempotence. La pipeline est **restartable** : chaque étape détecte ce qui reste à faire et saute ce qui est déjà fait. **`caffeinate -i` est lancé automatiquement** au démarrage sur macOS (subprocess lié au PID parent, meurt avec le run).

### Lancer la pipeline complète

```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --delete-pdf-after-success
```

Tous les chemins (`input/arks_revues.json`, `input/arks_numeros.json`, `pdf_process/`, `images_process/`) sont les défauts du script.

### Étapes isolées

```bash
# Étape 1 seule (revues → ARK numéros)
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step2 --disable-step3

# Étape 2 seule (ARK → PDFs Selenium)
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step3

# Étape 3 seule (PDF → images bitonales)
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step2 \
  --delete-pdf-after-success
```

### Défauts baked in (à ne pas réécrire)

| Option | Défaut |
|---|---|
| `--start-year` / `--end-year` | 1870 / 1914 |
| `--issues-rpm` | 5 |
| `--pdf-rpm` | 0.5 |
| `--image-rpm` | 30 |
| `--step{1,2,3}-cb-threshold` | 5 |
| `--step{1,2,3}-cb-sleep-seconds` | 600 |
| `--step{1,2,3}-cb-max-cooldowns` | 3 |
| `--dpi` | 300 |
| `--bitonal-threshold` | 180 |
| `--image-format` | png |
| caffeinate -i | auto (opt-out `--no-caffeinate`) |

### Options à connaître ponctuellement

- `--force-step1` / `--force-pdf` / `--force-images` : ignore l'idempotence pour l'étape choisie.
- `--show-browser` : affiche Firefox au lieu du headless (étape 2, debug).
- `--step2-cookies-file <path>` : cookies Firefox exportés, utile contre ALTCHA.
- `--step2-fail-fast-altcha` : stoppe si ALTCHA détecté.
- `--no-caffeinate` : désactive le caffeinate auto.

### Appel direct des sous-scripts

Chaque script est autonome :

- Étape 1 : `scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py`
- Étape 2 : `scripts/pipeline_pdf/selenium_scraping_pdf.py` — nécessite Firefox + geckodriver (`/opt/homebrew/bin/geckodriver` par défaut).
- Étape 3 : `scripts/pipeline_pdf/scraping_pdf_to_images.py` — nécessite `poppler` (pdfinfo/pdftoppm).

## État de sortie

- `input/arks_numeros.json` est **enrichi à chaque étape** : chaque item gagne `pdf_path`, `pdf_size_bytes`, `images_total`, `images_converted`, `status`, `error_stage`, etc.
- `manifest_iiif_process/state_pdf.json` : état de l'orchestrateur (runs, revues done/error).
- `tableau_arks_numeros.csv` : miroir à plat pour inspection rapide.

Voir [todo.md](todo.md) pour l'état courant revue par revue.

## Métadonnées bibliographiques (CSV pour le mémoire)

Script séparé : `scripts/scraping_notices_revues.py`. Pour chaque revue listée dans `input/arks_revues.json`, requête l'API `https://gallica.bnf.fr/services/OAIRecord?ark=<ark>` et parse la notice Dublin Core renvoyée. Produit un CSV `input/notices_revues.csv` avec une ligne par revue.

### Commande

```bash
.venv/bin/python -u scripts/scraping_notices_revues.py
```

Options :
- `--input` (défaut : `input/arks_revues.json`)
- `--output` (défaut : `input/notices_revues.csv`)
- `--requests-per-minute` (défaut : 5, comme l'étape 1)
- `--start-year` / `--end-year` (défaut : 1870 / 1914) — utilisé pour calculer `nb_issues_in_period`.
- `--user-agent`, `--timeout-seconds`.

### Colonnes du CSV produit

| Colonne | Source XML | Exemple |
|---|---|---|
| `nom_court` | clé du JSON d'entrée | `revue_scientifique` |
| `ark` / `ark_url` | URL pérenne | `ark:/12148/cb34378388w` |
| `title` | `dc:title` | `Revue scientifique` |
| `title_variants` | `dc:description` (préfixe « Variante(s) de titre ») | `Revue scientifique illustrée` |
| `publishers` | `dc:publisher` (joints par ` \| `) | `G. Baillière (Paris)` |
| `contributors` | `dc:contributor` | `Moureu, Charles (1863-1929). Éditeur scientifique` |
| `date_publication` / `date_start` / `date_end` | `dc:date` | `1884-1954`, `1884`, `1954` |
| `periodicity` | `dc:description` (préfixe « Périodicité ») | `Hebdomadaire (1884-1924)` |
| `collection_state` | `dc:description` (préfixe « Etat de collection ») | `3e sér., t. 7 (1884)-…` |
| `issn` | `dc:identifier` (extraction regex) | `03704556` |
| `language`, `subjects`, `types` | DC correspondants | `fre`, `Pathologie` |
| `dewey` / `sdewey` / `typedoc` | hors DC, balises Gallica | `5`, `50`, `fascicule` |
| `source` | `dc:source` | `Bibliothèque nationale de France` |
| `ensemble_documentaire` | `dc:description` (préfixe « Appartient à l'ensemble… ») | `FranceBr` |
| `catalog_url` | `dc:relation` (URL `catalogue.bnf.fr`) | http://catalogue.bnf.fr/ark:/… |
| `nb_total_views` | `dc:format` (« Nombre total de vues ») | `77317` |
| `first_indexation_date` | hors DC | `15/10/2007` |
| `years_available` / `nb_years_available` | balises `<date nbIssue="…">` | `1884\|1885\|…`, `68` |
| `nb_issues_total` | somme des `nbIssue` | `133` |
| `years_in_period` / `nb_years_in_period` / `nb_issues_in_period` | filtré sur `[start_year, end_year]` | `62` issues entre 1870 et 1914 |
| `fetch_status` / `fetch_error` | `ok`, `error`, `not_found` | — |

## Prérequis

- Python 3 + `.venv` avec : `requests`, `selenium`, `pdf2image`, `Pillow`.
- Binaires système : Firefox, `geckodriver`, `poppler` (pour `pdfinfo`, `pdftoppm`).
- Respect des limites API Gallica (cf. https://api.bnf.fr/fr/node/232) : 5‑10 req/min max pour l'API Issues, très lent pour le PDF.
