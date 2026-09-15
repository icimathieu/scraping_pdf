# scraping_pdf — collecte Gallica / IIIᵉ République (1870-1914)

## Structure du dépôt

```
scraping_pdf/
├── input/
│   ├── arks_revues.json                    # { nom_revue: ark_perenne } — entree primaire
│   ├── arks_numeros.json                   # numeros enrichis (sortie etape 1)
│   ├── arks_numeros_with_manifests.json    # numeros + manifestes IIIF + pages_total
│   └── _tmp_pdf/                           # tampon Selenium pour Firefox (gitignored)
├── scripts/
│   ├── pipeline_pdf/                       # pipeline PDF (numeros >= 500 pages)
│   │   ├── run_pipeline_gallica_pdf.py     #   orchestrateur (etapes 1+2+3, lance caffeinate)
│   │   ├── scraping_arks_numeros_gallica_pdf.py   # etape 1 : revues -> ARK numeros
│   │   ├── selenium_scraping_pdf.py        #   etape 2 : ARK numero -> PDF (Selenium/Firefox)
│   │   └── scraping_pdf_to_images.py       #   etape 3 : PDF -> PNG/TIFF bitonal
│   ├── pipeline_manifest_iiif/             # pipeline IIIF (numeros < 500 pages)
│   │   ├── run_pipeline_gallica.py         #   orchestrateur historique (peu utilise depuis la partition)
│   │   ├── scraping_arks_numeros_gallica.py       # etape 1 : revues -> ARK numeros (variante)
│   │   ├── scraping_manifest_gallica.py    #   etape 2 : ARK -> manifest.json IIIF
│   │   ├── analyze_pages_and_partition.py  #   etape 2.5 : extrait pages_total + propose partition PDF/IIIF
│   │   └── scraping_images_gallica.py      #   etape 3 : manifest -> page_XXXX.jpg bitonal full
│   ├── pipeline_presse/                       # inventaire IIIF de la presse generaliste
│   ├── pipeline_jorf/                      # pipeline JORF 1870-1880 (quotidien, voie IIIF)
│   │   ├── inventaire_jorf.py              #   etape 1 : Issues + Pagination (remplace le scraping des manifestes)
│   │   ├── construire_entree_jorf.py       #   etape 2 : deduplication par date + manifestes synthetiques
│   │   └── reparer_jorf.py                 #   etape 3 bis : comble les trous via l'autre numerisation
│   └── scraping_notices_revues.py          # metadonnees bibliographiques des revues (OAIRecord)
├── pdf_process/                            # sorties pipeline PDF (gitignored)
├── images_process/                         # sorties images (PDF→PNG ET IIIF→JPG) (gitignored)
├── manifest_iiif_process/                  # manifestes IIIF + state_pdf.json (gitignored sauf state)
├── pipeline_pdf.md                         # specification de la pipeline PDF
├── pipeline_manifest.md                    # specification de la pipeline IIIF + partition
├── pipeline_jorf.md                        # specification de la pipeline JORF 1870-1880
├── pipeline_presse.md                      # specification de l'inventaire presse 1870-1914
├── CLAUDE.md                               # conventions de commande (pour usage Claude Code)
├── requirements.txt                        # dependances Python pinnees
├── readme.md                               # ce fichier
└── LICENSE
```

Les fichiers de spécification détaillent chaque étape :
- [pipeline_pdf.md](pipeline_pdf.md) — pipeline PDF (gros numéros)
- [pipeline_manifest.md](pipeline_manifest.md) — pipeline IIIF + partition de page (petits numéros)
- [pipeline_jorf.md](pipeline_jorf.md) — pipeline JORF 1870-1880 (presse quotidienne, voie IIIF sans manifestes)
- [pipeline_presse.md](pipeline_presse.md) — inventaire IIIF de la presse généraliste (18 titres, 1870-1914)

## Vue d'ensemble

Le corpus cible : **45 revues**, **~9225 numéros** entre 1870 et 1914, soit **~884 000 pages** au total (estimation après scraping de 94 % des manifestes IIIF).

Deux pipelines coexistent, complémentaires :

| Pipeline | Source primaire | Sortie | Usage actuel |
|---|---|---|---|
| **PDF** (`scripts/pipeline_pdf/`) | `https://gallica.bnf.fr/{ark}.pdf` (téléchargement Selenium/Firefox) | PNG/TIFF bitonal | **Gros numéros (≥500 pages)** — environ 10 % des numéros mais 73 % du volume |
| **IIIF** (`scripts/pipeline_manifest_iiif/`) | `https://gallica.bnf.fr/iiif/{ark}/manifest.json` + Image API en bitonal full | JPG bitonal | **Petits numéros (<500 pages)** — environ 90 % des numéros, 27 % du volume |
| **JORF** (`scripts/pipeline_jorf/`) | services `Issues` + `Pagination`, puis Image API | JPG 2864 px | **Journal officiel 1870-1880** — ~3 700 numéros quotidiens, ~97 600 pages |

La partition à 500 pages est calculée à partir des manifestes IIIF par `analyze_pages_and_partition.py`. 

## Installation rapide

```bash
git clone https://github.com/icimathieu/scraping_pdf.git
cd scraping_pdf

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Dépendances système (non installées par pip) :

| Binaire | Rôle | Installation macOS |
|---|---|---|
| Firefox | étape PDF (Selenium) | https://www.mozilla.org/firefox/ |
| `geckodriver` | pilote Selenium pour Firefox | `brew install geckodriver` |
| `poppler` | conversion PDF → images (`pdfinfo`, `pdftoppm`) | `brew install poppler` |

Sur Linux : `apt install firefox-esr poppler-utils` puis [installer geckodriver](https://github.com/mozilla/geckodriver/releases).


## Pipeline PDF — usage

L'orchestrateur enchaîne les 3 étapes (revues → numéros → PDF → images bitonales) avec **reprise et idempotence** : chaque étape détecte ce qui reste à faire. `caffeinate -i` est lancé automatiquement sur macOS pour empêcher la mise en veille du Mac pendant les longs runs.

```bash
# Pipeline complete
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --delete-pdf-after-success

# Etapes isolees
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step2 --disable-step3
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step1 --disable-step3
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step1 --disable-step2 --delete-pdf-after-success
```

Voir [pipeline_pdf.md](pipeline_pdf.md) pour les paramètres détaillés (cadences, timeouts, circuit breaker).

## Pipeline IIIF — usage

Les manifestes IIIF de tous les numéros sont d'abord récupérés (étape 2 manifest), puis `analyze_pages_and_partition.py` produit deux JSON de partition. Les images bitonales des numéros de moins de 500 pages sont ensuite téléchargées via l'API IIIF Image.

```bash
# Etape 2 manifest (1 req/min recommande pour respecter Gallica)
.venv/bin/python -u scripts/pipeline_manifest_iiif/scraping_manifest_gallica.py \
  --input input/arks_numeros.json \
  --output input/arks_numeros_with_manifests.json \
  --manifest-root manifest_iiif_process \
  --requests-per-minute 1

# Partition PDF / IIIF a seuil 500 pages
.venv/bin/python -u scripts/pipeline_manifest_iiif/analyze_pages_and_partition.py \
  --threshold 500

# Etape 3 images IIIF (4-5 req/min documentes par BnF en mode full)
.venv/bin/python -u scripts/pipeline_manifest_iiif/scraping_images_gallica.py \
  --requests-per-minute 4
```

Voir [pipeline_manifest.md](pipeline_manifest.md) pour le détail de la partition et de l'étape 3.

## Métadonnées bibliographiques (CSV séparé)

Script indépendant : `scripts/scraping_notices_revues.py`. Pour chaque revue listée dans `input/arks_revues.json`, requête l'API `https://gallica.bnf.fr/services/OAIRecord?ark=<ark>` et parse la notice Dublin Core. Produit `input/notices_revues.csv` (une ligne par revue, ~30 colonnes : title, publishers, periodicity, ISSN, années disponibles, etc.).

```bash
.venv/bin/python -u scripts/scraping_notices_revues.py
```

Options : `--input`, `--output`, `--requests-per-minute`, `--start-year`, `--end-year`, `--user-agent`, `--timeout-seconds`.

## État de sortie

À chaque étape, `arks_numeros.json` (ou `arks_numeros_with_manifests.json`) est **enrichi** avec de nouveaux champs par item : `pdf_path`, `pdf_size_bytes`, `manifest_path`, `pages_total`, `images_total`, `images_converted`, `status`, `error_stage`, `error_code`, `error_message`. Chaque pipeline maintient aussi son `state_*.json` pour la reprise après interruption.

Les sorties volumineuses (`pdf_process/`, `images_process/`, manifestes IIIF) sont gitignored. Seuls les JSON d'entrée et de partition sont versionnés.

## Cadences de requêtage (résumé pratique)

Mesurées empiriquement face au throttling de Gallica (mai 2026) :

| API | Documentation BnF | Cadence soutenable | Notes |
|---|---|---|---|
| Issues (étape 1) | 10/min | 5-10/min | Très permissif, ne déclenche jamais de 429. |
| Manifest IIIF (étape 2 manifest) | non documenté | **1 req/min** | Plus strict que l'Image API en pratique. Burst ~20 puis throttle dur. |
| Image IIIF full bitonal | 5/min (phase transitoire) | **4-5/min** | Limite documentée, confirmée par benchmark (0 % 429 à 4/min, 28 % à 6/min). |
| Pagination (`services/Pagination`) | non documenté | **40 req/min** | Mesuré sans un seul 429 (45 requêtes, 09/2026). Renvoie `nbVueImages`, égal au nombre de canvas du manifeste — permet d'éviter le scraping des manifestes. |
| PDF Gallica (étape 2 PDF) | non documenté | **1 PDF / 5 min** | Très conservateur. Pas une API documentée, comportement instable. |

Ces valeurs sont les défauts des scripts. Le circuit breaker arrête le run après N cooldowns consécutifs.

⚠️ **Gallica throttle en 404, pas seulement en 429.** Une rafale de requêtes sur l'Image API fait répondre 404 à des pages qui existent bel et bien : elles reviennent en 200 après quelques minutes de pause. Ne jamais conclure qu'une page est absente du corpus sur la foi d'une sonde rapprochée — seule la convergence du scraper à cadence nominale fait foi.

## Licence

Voir [LICENSE](LICENSE).