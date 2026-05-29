Pipeline Gallica IIIF (4 etapes + partition)
============================================

Objectif global
---------------
- A partir d'ARK de revues, produire la liste des ARK de numeros sur une periode donnee.
- Pour chaque numero, recuperer le manifest IIIF (decrit la liste des canvases =
  pages disponibles).
- Calculer le nombre de pages par numero et partitionner le corpus en deux
  ensembles : gros numeros (>= seuil) traites par la pipeline PDF, petits numeros
  (< seuil) traites par la pipeline IIIF Image.
- Pour les petits numeros : telecharger les images bitonales "full" page par page
  via l'IIIF Image API.
- Industrialiser le tout dans un orchestrateur avec reprise.


ETAPE 1 - Revue ARK -> ARK de tous les numeros
----------------------------------------------
Script de reference:
- scripts/pipeline_manifest_iiif/scraping_arks_numeros_gallica.py

Identique a l'etape 1 de la pipeline PDF dans son comportement, mais ecrit dans le
meme JSON (les deux pipelines partagent input/arks_numeros.json).

Entree:
- input/arks_revues.json
- Format attendu : JSON objet { "nom_revue": "ark_ou_url_perenne" }

Sorties:
- JSON : input/arks_numeros.json
- CSV  : input/tableau_arks_numeros.csv

Parametres CLI principaux (defauts):
- --input                  input/arks_revues.json
- --output-json            input/arks_numeros.json
- --output-csv             input/tableau_arks_numeros.csv
- --start-year             1870
- --end-year               1914
- --requests-per-minute    10
- --user-agent             "memoire-gallica-scraper/1.0 (+contact-local)"
- --cb-threshold           5
- --cb-sleep-seconds       600

Logique (voir aussi pipeline_pdf.md pour les details) :
1. Normalisation des ARK de revue ("ark:/12148/..." + suffixe "/date").
2. GET https://gallica.bnf.fr/services/Issues?ark=<ark_revue_date>, parsing XML,
   extraction des balises <year>.
3. Pour chaque annee dans [start_year, end_year] : GET de la liste des fascicules,
   parsing des <issue> et construction du numero_id.
4. Ecriture JSON + CSV.

Commande:
  .venv/bin/python -u scripts/pipeline_manifest_iiif/scraping_arks_numeros_gallica.py


ETAPE 2 - ARK de numero -> manifest.json IIIF
---------------------------------------------
Script de reference:
- scripts/pipeline_manifest_iiif/scraping_manifest_gallica.py

Entree:
- input/arks_numeros.json (structure avec items[])

Sorties:
- JSON enrichi : input/arks_numeros_with_manifests.json (le nom par defaut peut
  varier selon la commande -- ne pas ecraser arks_numeros.json par precaution)
- CSV miroir  : input/tableau_arks_numeros_with_manifests.csv
- Manifests sur disque : manifest_iiif_process/<revue>/<numero_id>.manifest.json

Parametres CLI principaux (defauts) :
- --input                  input/arks_numeros.json
- --output                 input/arks_numeros.json
- --output-csv             input/tableau_arks_numeros.csv
- --manifest-root          manifest_iiif_process
- --requests-per-minute    4            (defaut script ; en production : 1/min)
- --timeout-seconds        30
- --cb-threshold           3
- --cb-sleep-seconds       600
- --jitter-seconds         1.5          (jitter aleatoire pour eviter le burst regulier)
- --save-every             25           (sauvegarde du JSON tous les N items)
- --user-agent             "Mozilla/5.0 ... Firefox/150.0"  (UA natif Firefox)
- --force                  (re-telechargement des manifests existants)

Cadence empirique (mai 2026) :
- La documentation BnF (https://api.bnf.fr/fr/node/232) ne specifie pas de limite
  explicite pour l'endpoint manifest IIIF.
- Mesures sur ~9000 items : burst d'environ 20 manifestes acceptes, puis throttle
  dur (429 ou erreur 503). La cadence soutenable propre est de **1 req/min**.
- A 4 req/min : ~30 % d'erreurs 429 apres 200 manifestes.
- A 3 req/min : 1-2 cooldowns par centaine d'items.
- A 1 req/min : 0 erreur 429 sur 5500 manifestes consecutifs (validation 22-26/05).

Logique :
1. Lecture de input/arks_numeros.json, resolution de items[].
2. requests.Session avec Retry(total=0) (pas de retry urllib3 : tous les
   re-essais passent par le circuit breaker pour ne pas court-circuiter le
   rate limiter).
3. Rate limiter local en fenetre glissante (60s) + jitter aleatoire 0-1.5s avant
   chaque requete.
4. Pour chaque item :
   - URL = https://gallica.bnf.fr/iiif/<issue_ark>/manifest.json
   - manifest_path = manifest_iiif_process/<revue_sanitized>/<numero_id_sanitized>.manifest.json
   - Si le manifest existe deja et pas --force : skip ("skipped" dans les compteurs).
   - Sinon : GET avec timeout, raise_for_status, ecriture sur disque.
5. Mise a jour item : manifest_url, manifest_path, status, error_*.
6. Sauvegarde du JSON tous les --save-every items (et a la fin).

Circuit breaker :
- TRANSIENT_STATUS_CODES = (429, 500, 502, 503, 504)
- Apres --cb-threshold echecs consecutifs : cooldown de --cb-sleep-seconds.
- Les 404 et autres 4xx (sauf 429) sont consideres comme erreurs structurelles
  (resource absente) : reportees mais ne declenchent pas le cooldown.

Commande de production (1 req/min + watchdog) :
  .venv/bin/python -u scripts/pipeline_manifest_iiif/scraping_manifest_gallica.py \
      --input input/arks_numeros.json \
      --output input/arks_numeros_with_manifests.json \
      --output-csv input/tableau_arks_numeros_with_manifests.csv \
      --manifest-root manifest_iiif_process \
      --requests-per-minute 1

Un watchdog optionnel peut surveiller le log et tuer le run si trop de cooldowns
ou de 429 : voir le snippet a la fin de ce document.


ETAPE 2.5 - Analyse des pages et partition PDF / IIIF
-----------------------------------------------------
Script de reference:
- scripts/pipeline_manifest_iiif/analyze_pages_and_partition.py

Objectif :
- Extraire pages_total = nombre de canvases de chaque manifest IIIF.
- Enrichir input/arks_numeros_with_manifests.json avec ce champ.
- Proposer une partition PDF (gros numeros) / IIIF (petits numeros) au seuil donne.
- Aucun appel reseau : lecture pure des fichiers manifest_iiif_process/.

Parametres CLI principaux :
- --input                  input/arks_numeros_with_manifests.json
- --output                 input/arks_numeros_with_manifests.json (reecriture)
- --output-pdf             input/arks_partition_pdf.json
- --output-iiif            input/arks_partition_iiif.json
- --manifest-root          manifest_iiif_process
- --threshold              500 (valeur de production retenue)

Logique :
1. Pour chaque item du JSON : lecture du manifest local et extraction de pages_total :
   - IIIF Presentation v2 : len(sequences[0].canvases)
   - IIIF Presentation v3 : len(items)
2. Ecriture du JSON enrichi.
3. Affichage de statistiques de distribution (quartiles, percentiles, histogramme).
4. Proposition de seuils candidats (100, 150, 200, 250, 300, 400, 500, 750, 1000)
   avec, pour chacun : nombre de numeros qui basculent en PDF, total pages en PDF,
   total pages en IIIF, temps estime IIIF a 4 img/min.
5. Ecriture des deux JSON de partition au seuil retenu.

Distribution observee (8691 manifestes au 29/05/2026) :
- Mediane : 20 pages
- Moyenne : 102 pages
- P90 : 464 pages, P95 : 638 pages
- Distribution bimodale : 77 % des numeros < 50 pages (fascicules courts), puis un
  creux entre 150 et 500 pages, puis ~9 % a 500-1000 pages (volumes annuels relies).

Partition retenue (seuil 500) :
- PDF (>= 500 p.) : 846 numeros (10 %), ~645 000 pages -> ~8 jours a 100 PDF/jour.
- IIIF (< 500 p.) : 7845 numeros (90 %), ~239 000 pages -> ~42 jours a 4 img/min
  (~33 jours a 5 img/min).

Commande :
  .venv/bin/python -u scripts/pipeline_manifest_iiif/analyze_pages_and_partition.py --threshold 500


ETAPE 3 - manifest IIIF -> images bitonales full
------------------------------------------------
Script de reference:
- scripts/pipeline_manifest_iiif/scraping_images_gallica.py

Objectif :
- Pour chaque numero de la partition IIIF (< seuil), telecharger les images
  bitonales "full" page par page via l'IIIF Image API.

Entree :
- input/arks_partition_iiif.json (sous-ensemble produit par analyze_pages_and_partition)
- Manifests deja sur disque dans manifest_iiif_process/

Sorties :
- Images : images_process/<revue>/<numero_id>/page_0001.jpg ...
- JSON enrichi avec compteurs images.

Parametres CLI principaux (defauts) :
- --input                  input/arks_numeros.json
- --output                 input/arks_numeros.json
- --output-csv             input/tableau_arks_numeros.csv
- --manifest-root          manifest_iiif_process
- --image-root             images_process
- --requests-per-minute    4            (limite documentee BnF : 5/min en full)
- --timeout-seconds        30
- --jitter-seconds         1.5
- --cb-threshold           5
- --cb-sleep-seconds       600
- --cb-max-cooldowns       2
- --quality                bitonal      (choix : bitonal | gray | color)
- --format                 jpg
- --max-pages              0            (0 = toutes les pages)
- --user-agent             "Mozilla/5.0 ... Firefox/150.0"
- --force

Cadence empirique :
- Documentation BnF "phase transitoire" : 5 req/min maximum sur l'IIIF Image API en
  mode full ou taille > 1000 px, bande passante 832 Ko/s par client.
- Benchmark realise en mai 2026 (180s par palier) :
  - 4 req/min : 0 % 429
  - 6 req/min : 28 % 429
- Production : on s'aligne sur **4 req/min** (marge de securite vs. la limite
  documentee a 5).

Logique :
1. Resolution du manifest par item (priorite manifest_path, sinon fallback).
2. Parsing du manifest : extraction des canvases (v2 ou v3).
3. Pour chaque canvas : extraction de service.@id (ou fallback resource.@id) et
   construction de l'URL <service_id>/full/full/0/bitonal.jpg.
4. Telechargement avec rate limit 4/min + jitter, retry/backoff sur 429/5xx,
   skip si fichier existe et taille > 0 (sauf --force).
5. Maj des compteurs item : images_total, images_downloaded, images_existing,
   images_errors, image_output_dir.

Commande :
  .venv/bin/python -u scripts/pipeline_manifest_iiif/scraping_images_gallica.py \
      --input input/arks_partition_iiif.json \
      --requests-per-minute 4


ETAPE 4 - Orchestrateur historique (peu utilise depuis la partition)
--------------------------------------------------------------------
Script de reference:
- scripts/pipeline_manifest_iiif/run_pipeline_gallica.py

Cet orchestrateur a ete concu pour enchainer les 3 etapes (revues -> manifestes ->
images) avec reprise et idempotence. Depuis la mise en place de la partition au
seuil 500, on l'utilise rarement : on prefere lancer chaque etape isolement avec
ses propres cadences, et inserer entre 2 et 3 le script de partition.

Parametres CLI principaux (defauts) :
- --revues-input           input/arks_revues.json
- --numeros-json           input/arks_numeros.json
- --numeros-csv            input/tableau_arks_numeros.csv
- --manifest-root          manifest_iiif_process
- --image-root             images_process
- --state-file             manifest_iiif_process/state.json
- --issues-rpm             10
- --manifest-rpm           5
- --image-rpm              5
- --step{1,2,3}-cb-threshold       5
- --step{1,2,3}-cb-sleep-seconds   600
- --timeout-manifest       15
- --timeout-image          20
- --quality                bitonal
- --format                 jpg
- --max-pages              0
- --resume                 True
- --force-step1 / --force-manifests / --force-images
- --disable-step1 / --disable-step2 / --disable-step3

NOTE : les defauts de cadence de cet orchestrateur (manifest-rpm 5, image-rpm 5)
sont historiques et ne refletent plus la cadence soutenable empirique du manifest
(1/min). En usage actuel, lancer les sous-scripts directement avec les cadences
ajustees est plus fiable.


Watchdog optionnel (etape 2 manifest)
-------------------------------------
Pour les longs runs (etape 2 manifest sur ~9000 items, ~6 jours a 1/min), un
watchdog en bash peut surveiller le log et tuer le run si Gallica throttle trop.

Exemple (a placer dans /tmp/iiif_watchdog.sh, executable et lance en background) :

  #!/bin/bash
  LOG=/tmp/iiif_step2_full.log
  WLOG=/tmp/iiif_watchdog.log
  CHECK_INTERVAL=300
  MAX_CB_COOLDOWNS=10
  MAX_429=150

  ts() { date "+%Y-%m-%d %H:%M:%S"; }

  echo "[$(ts)] watchdog demarre - cb>=${MAX_CB_COOLDOWNS} ou 429>=${MAX_429}" >> "$WLOG"

  while true; do
    sleep "$CHECK_INTERVAL"
    if ! pgrep -f scraping_manifest_gallica.py >/dev/null 2>&1; then
      echo "[$(ts)] process absent -> watchdog s'arrete." >> "$WLOG"; exit 0
    fi
    [ -f "$LOG" ] || { echo "[$(ts)] log absent" >> "$WLOG"; continue; }
    cb=$(grep -cF '] Sleeping ' "$LOG")
    err429=$(grep -cF 'kind=429' "$LOG")
    prog=$(grep -F '[INFO][progress]' "$LOG" | tail -1)
    echo "[$(ts)] cb=${cb} err429=${err429} | ${prog}" >> "$WLOG"
    if [ "${cb:-0}" -ge "$MAX_CB_COOLDOWNS" ] || [ "${err429:-0}" -ge "$MAX_429" ]; then
      echo "[$(ts)] SEUIL DEPASSE -> ARRET" >> "$WLOG"
      pkill -TERM -f scraping_manifest_gallica.py
      sleep 5
      pkill -KILL -f scraping_manifest_gallica.py 2>/dev/null
      exit 1
    fi
  done


Notes transverses
-----------------
- Toujours HTTPS.
- L'endpoint manifest IIIF est plus strict que l'endpoint Image (empirique).
  Diagnostic : un run de scraping de manifestes a 4/min se faisait throttler
  pendant qu'un benchmark Image API a 4/min restait clean en parallele.
- Le format des manifestes Gallica est principalement IIIF Presentation v2
  (sequences[0].canvases), parfois v3 (items).
- Sur les ConnectionError (reseau coupe) : le scraper logue et incremente le
  circuit breaker comme pour un 5xx, sans crasher. La streak se reset des qu'une
  requete reussit.
