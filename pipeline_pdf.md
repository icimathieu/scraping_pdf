Pipeline Gallica PDF (4 etapes)
===============================

Objectif global
---------------
- A partir d'ARK de revues, produire la liste des ARK de numeros sur une periode donnee.
- Pour chaque numero, telecharger le PDF Gallica (via Selenium/Firefox pour passer ALTCHA).
- Convertir les PDF en images bitonales (PNG ou TIFF), page par page.
- Industrialiser le tout dans un orchestrateur avec reprise et idempotence.

Cette pipeline est utilisee pour les **gros numeros (>= 500 pages)** dans la partition courante.
Pour les petits numeros, voir pipeline_manifest.md.


ETAPE 1 - Revue ARK -> ARK de tous les numeros
----------------------------------------------
Script de reference:
- scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py

Entree:
- input/arks_revues.json
- Format attendu: JSON objet { "nom_revue": "ark_ou_url_perenne" }

Sorties:
- JSON: input/arks_numeros.json
- CSV:  input/tableau_arks_numeros.csv

Parametres CLI principaux (defauts):
- --input                  input/arks_revues.json
- --output-json            input/arks_numeros.json
- --output-csv             input/tableau_arks_numeros.csv
- --start-year             1870
- --end-year               1914
- --requests-per-minute    5
- --user-agent             "memoire-gallica-scraper/1.0 (+contact-local)"
- --cb-threshold           5
- --cb-sleep-seconds       600
- --cb-max-cooldowns       3

Logique:
1. Chargement et normalisation de chaque ARK de revue ("ark:/12148/..." + suffixe "/date").
2. Pour chaque revue : GET https://gallica.bnf.fr/services/Issues?ark=<ark_revue_date>,
   parsing XML, extraction des balises <year> filtrees sur [start_year, end_year].
3. Pour chaque annee retenue : GET https://gallica.bnf.fr/services/Issues?ark=<ark>&date=<annee>,
   parsing des <issue> avec attributs ark, dayOfYear, precision.
4. Construction du numero_id :
   - si la precision contient YYYY/MM/DD : numero_id = <revue> + YYYYMMDD
   - sinon : numero_id = <revue><annee><dayOfYear>_<id_ark_court>
5. Ecriture du JSON enrichi + CSV miroir.

Robustesse:
- requests.Session avec Retry(total=2, backoff_factor=1.0) sur 429/5xx.
- Rate limiter local en fenetre glissante (60s).
- Circuit breaker (3 cooldowns max de 600s sur erreurs transitoires).

Commande d'execution directe:
  .venv/bin/python -u scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py


ETAPE 2 - ARK de numero -> PDF Gallica (Selenium)
-------------------------------------------------
Script de reference:
- scripts/pipeline_pdf/selenium_scraping_pdf.py

Pourquoi Selenium plutot qu'un GET direct sur .pdf : depuis fin 2025 Gallica protege
l'endpoint PDF par un challenge ALTCHA en cas de detection de comportement bot,
qui se valide soit en interactif soit via un cookie de session importe depuis
un Firefox utilisateur.

Entree:
- input/arks_numeros.json (structure avec items[])

Sorties:
- JSON enrichi : input/arks_numeros.json
- CSV enrichi  : input/tableau_arks_numeros.csv
- PDFs sur disque : pdf_process/<revue>/<numero_id>/<numero_id>.pdf

Parametres CLI principaux (defauts):
- --input                       input/arks_numeros.json
- --output                      input/arks_numeros.json
- --output-csv                  input/tableau_arks_numeros.csv
- --pdf-root                    pdf_process
- --requests-per-minute         0.2          (1 PDF toutes les 5 minutes)
- --page-timeout-seconds        60           (chargement de la page Gallica)
- --timeout-seconds             1800         (timeout total de telechargement, 30 min)
- --stalled-timeout-seconds     120          (abandon si le .part ne grossit plus depuis 2 min)
- --progress-log-seconds        10
- --cb-threshold                2            (plus strict que les autres etapes)
- --cb-sleep-seconds            3600         (1 h de cooldown, laisser Gallica retomber)
- --cb-max-cooldowns            2
- --cookies-file                gallica.bnf.fr_cookies.txt
- --user-agent                  ""           (vide = UA natif de Firefox, important contre ALTCHA)
- --geckodriver-path            /opt/homebrew/bin/geckodriver
- --firefox-binary-path         /Applications/Firefox.app/Contents/MacOS/firefox
- --fail-fast-altcha            (flag : stoppe immediatement si ALTCHA detecte)
- --show-browser                (flag : affiche Firefox au lieu du headless)
- --force                       (flag : re-telechargement des PDF existants)

Logique:
1. Lancement de Firefox via geckodriver (headless par defaut).
2. Si --cookies-file existe, import des cookies dans la session Selenium (necessaire
   pour bypasser ALTCHA en non-interactif). Le format attendu est le format Mozilla
   cookies.txt, exporte depuis l'extension "Cookie Quick Manager" ou similaire.
3. Pour chaque item :
   - Construction de pdf_url = https://gallica.bnf.fr/<issue_ark>.pdf
   - Construction du pdf_path local
   - Si le PDF existe deja (taille > 0) et pas --force : skip
   - Sinon : ouverture de l'URL dans Selenium, attente du .part dans le dossier
     de telechargement, verification de la signature PDF et de l'absence de page
     HTML/ALTCHA/429, deplacement vers pdf_path.
4. Mise a jour de l'item : pdf_url, pdf_path, pdf_size_bytes, status, error_*.

Detection ALTCHA :
- Si la page chargee contient le marqueur ALTCHA (CSS id "customAltcha_checkbox_*"),
  Selenium tente le clic. Si --fail-fast-altcha : on s'arrete au premier ALTCHA.
- Si le challenge ne peut pas etre resolu : error_code = "captcha_required".

Detection des decrochages reseau :
- Si la taille du .part ne grossit plus pendant --stalled-timeout-seconds (2 min)
  alors que --timeout-seconds n'est pas atteint, on tue le telechargement et on passe
  au numero suivant (connexion coupee = inutile d'attendre les 30 min).

Commande d'execution directe:
  .venv/bin/python -u scripts/pipeline_pdf/selenium_scraping_pdf.py


ETAPE 3 - PDF -> images bitonales
----------------------------------
Script de reference:
- scripts/pipeline_pdf/scraping_pdf_to_images.py

Objectif:
- Rendre chaque page d'un PDF en image bitonale 1-bit (PNG ou TIFF) prete pour OCR.
- Conversion 100% locale via Poppler (pdftoppm), pas de requete reseau.

Entree:
- input/arks_numeros.json (items avec pdf_path)

Sorties:
- JSON enrichi : input/arks_numeros.json
- CSV enrichi  : input/tableau_arks_numeros.csv
- Images : images_process/<revue>/<numero_id>/page_XXXX.png (ou .tif)

Parametres CLI principaux (defauts):
- --input                  input/arks_numeros.json
- --output                 input/arks_numeros.json
- --output-csv             input/tableau_arks_numeros.csv
- --pdf-root               pdf_process
- --image-root             images_process
- --requests-per-minute    120          (throttling local de conversion)
- --cb-threshold           5
- --cb-sleep-seconds       600
- --cb-max-cooldowns       3
- --dpi                    300          (200 minimum pour OCR Tesseract, 300 recommande)
- --bitonal-threshold      180          (seuillage gris -> 1-bit)
- --image-format           png          (choix : png | tiff)
- --first-page             1
- --last-page              0            (0 = derniere page)
- --poppler-path           ""           (auto si poppler dans le PATH)
- --delete-pdf-after-success           (flag : supprime le PDF apres conversion reussie)
- --force                              (flag : reconvertit meme si l'image existe)

Logique:
1. Resolution du PDF : priorite a item.pdf_path, sinon fallback <pdf_root>/<numero_id>/<numero_id>.pdf.
2. Lecture metadata via pdfinfo_from_path (verifie la presence de Poppler).
3. Conversion page par page via pdf2image (fmt=ppm) :
   - rendu DPI
   - conversion niveaux de gris (Pillow)
   - seuillage bitonal 1-bit
   - sauvegarde PNG optimise OU TIFF compression Group4
   - skip si l'image existe deja (sauf --force)
4. Maj des compteurs item : images_total, images_converted, images_existing, images_errors.
5. Si --delete-pdf-after-success : le PDF est supprime apres conversion complete sans erreur.

Erreurs normalisees:
- pdf_to_jpg_source / pdf_not_found
- pdf_to_jpg_pdfinfo / pdfinfo_not_installed / pdf_page_count_error / pdf_syntax_error
- pdf_to_jpg_convert / pdf_to_jpg_save

Commande d'execution directe:
  .venv/bin/python -u scripts/pipeline_pdf/scraping_pdf_to_images.py


ETAPE 4 - Orchestrateur
-----------------------
Script de reference:
- scripts/pipeline_pdf/run_pipeline_gallica_pdf.py

Role:
- Enchainer automatiquement les 3 etapes avec reprise et idempotence.
- Lancer caffeinate -i (subprocess lie au PID parent, meurt avec le run) sur macOS
  pour empecher la mise en veille pendant les longs scrapings.
- Detecter ce qui reste a faire a chaque etape avant de lancer le sous-script.

Entrees:
- input/arks_revues.json
- input/arks_numeros.json
- input/tableau_arks_numeros.csv

Sorties:
- input/arks_numeros.json (mis a jour)
- input/tableau_arks_numeros.csv (mis a jour)
- manifest_iiif_process/state_pdf.json (etat persistant : runs, revues done/error)
- pdf_process/... et images_process/...

Parametres CLI principaux (defauts) :

Periode :
- --start-year                    1870
- --end-year                      1914

Cadences :
- --issues-rpm                    5            (etape 1)
- --pdf-rpm                       0.2          (etape 2, 1 PDF / 5 min)
- --image-rpm                     30           (etape 3, throttling de conversion locale)

Circuit breaker etape 1 :
- --step1-cb-threshold            5
- --step1-cb-sleep-seconds        600
- --step1-cb-max-cooldowns        3

Circuit breaker etape 2 (plus strict, decroachages reseau frequents) :
- --step2-cb-threshold            2
- --step2-cb-sleep-seconds        3600
- --step2-cb-max-cooldowns        2

Circuit breaker etape 3 :
- --step3-cb-threshold            5
- --step3-cb-sleep-seconds        600
- --step3-cb-max-cooldowns        3

Etape 2 (Selenium specifique) :
- --step2-page-timeout-seconds    60
- --timeout-pdf                   1800
- --step2-stalled-timeout-seconds 120
- --step2-progress-log-seconds    10
- --step2-cookies-file            gallica.bnf.fr_cookies.txt  (auto si present)
- --step2-fail-fast-altcha
- --show-browser

Etape 3 (conversion locale) :
- --dpi                           300
- --bitonal-threshold             180
- --image-format                  png
- --poppler-path                  ""
- --delete-pdf-after-success

Generaux :
- --user-agent                    ""           (vide = Firefox natif)
- --no-caffeinate                 (opt-out du caffeinate auto sur macOS)
- --disable-step1 / --disable-step2 / --disable-step3
- --force-step1   / --force-pdf   / --force-images

Logique :
1. Initialisation du run : chargement de state_pdf.json (ou init), ajout d'une entree
   runs[] avec status="running".
2. Etape 1 conditionnelle : detecte les revues a (re)calculer (nouvelle revue, ARK
   modifie, status != done, ou --force-step1). Merge intelligent du resultat.
3. Etape 2 conditionnelle : detecte les items sans pdf_path / fichier absent / vide,
   ou --force-pdf. Lance selenium_scraping_pdf.py.
4. Etape 3 conditionnelle : detecte les items avec images_total==0 OU
   images_errors>0 OU converted+existing < total OU --force-images.
5. Finalisation : status final (done/error selon return codes), finished_at,
   sauvegarde state_pdf.json.

Commandes d'execution type (voir aussi CLAUDE.md) :

Pipeline complete (PDF + conversion + suppression des PDFs apres succes) :
  .venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
      --delete-pdf-after-success

Etapes isolees :
  .venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step2 --disable-step3
  .venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step1 --disable-step3
  .venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --disable-step1 --disable-step2 \
      --delete-pdf-after-success


Notes transverses
-----------------
- Toujours HTTPS.
- User-Agent : pour l'etape 2 PDF, laisser vide pour que Firefox envoie son UA natif
  (important contre les mismatch ALTCHA quand les cookies viennent du meme Firefox).
- Le PDF Gallica n'est pas une API documentee : la cadence "1 PDF / 5 min" a ete
  trouvee empiriquement et reste tres conservatrice. Le throttle se manifeste par
  des 429 sur l'endpoint .pdf ou par des challenges ALTCHA persistants.
- En cas de longue interruption reseau pendant l'etape 2, le scraper logue des
  ConnectionError mais ne crashe pas : il les compte dans le circuit breaker. Si la
  streak depasse cb-threshold, un cooldown se declenche. Apres cb-max-cooldowns
  consecutifs, le run s'arrete proprement.
