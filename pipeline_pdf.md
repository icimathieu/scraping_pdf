Pipeline Gallica PDF (4 etapes)
===============================

Objectif global
---------------
- A partir d'ARK de revues, produire la liste complete des ARK de numeros sur une periode donnee.
- Pour chaque numero, telecharger le PDF Gallica.
- Convertir les PDF en images bitonales (PNG ou TIFF), page par page.
- Industrialiser l'ensemble dans un orchestrateur avec reprise/idempotence.


ETAPE 1 - Revue ARK -> ARK de tous les numeros (deja codee)
-----------------------------------------------------------
Script de reference:
- scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py

Entree:
- input/arks_revues.json
- Format attendu: JSON objet { "nom_revue": "ark_ou_url_perenne" }

Sorties:
- JSON: input/arks_numeros.json
- CSV:  input/tableau_arks_numeros.csv

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input (defaut: input/arks_revues.json)
     - --output-json (defaut: input/arks_numeros.json)
     - --output-csv (defaut: input/tableau_arks_numeros.csv)
     - --start-year (defaut: 1870)
     - --end-year (defaut: 1914)
     - --requests-per-minute (defaut: 10)
     - --user-agent (defaut: memoire-gallica-scraper/1.0 (+contact-local))
     - --cb-threshold (defaut: 5)
     - --cb-sleep-seconds (defaut: 600)

2. Chargement du fichier revues.
   - Lecture JSON avec json.load.
   - Validation implicite du format dictionnaire nom->ark/url.

3. Normalisation de l'ARK de revue.
   - Extraction de la forme "ark:/12148/xxxx" meme si la valeur source est une URL.
   - Ajout du suffixe "/date" si absent.
   - Resultat attendu pour l'API Issues: "ark:/12148/.../date".

4. Initialisation HTTP robuste.
   - requests.Session.
   - Retry automatique sur erreurs transitoires:
     - codes: 429, 500, 502, 503, 504
     - retries reseau connect/read
     - backoff exponentiel.
   - timeout de requete (30s dans le script actuel).
   - User-Agent explicite.

5. Application d'un rate limiting client.
   - Limiteur local en fenetre glissante (60s).
   - Maximum configurable de requetes/minute (defaut 10).
   - Attente automatique avant chaque appel si la fenetre est pleine.

6. Recuperation des annees de parution pour chaque revue.
   - Appel:
     - GET https://gallica.bnf.fr/services/Issues?ark=<ark_revue_date>
   - Parsing XML de la reponse.
   - Extraction de toutes les balises <year>.
   - Conversion en entiers et tri.

7. Filtrage de la periode.
   - Conservation des annees y telles que:
     - start_year <= y <= end_year
   - Cas courant:
     - 1870 <= y <= 1914.

8. Recuperation des fascicules annee par annee.
   - Pour chaque annee retenue:
     - GET https://gallica.bnf.fr/services/Issues?ark=<ark_revue_date>&date=<annee>
   - Parsing XML.
   - Pour chaque balise <issue>:
     - lecture attribut ark (ARK du numero)
     - lecture attribut dayOfYear (si present)
     - lecture texte libre "precision".

9. Normalisation des ARK de numero.
   - Si l'attribut issue ark ne contient pas le prefixe complet:
     - reconstruction "ark:/12148/<id>".

10. Construction d'un identifiant "numero_id".
    - Si la precision contient une date "YYYY/MM/DD":
      - numero_id = <nom_revue> + YYYYMMDD.
    - Sinon fallback deterministic:
      - numero_id = <nom_revue><annee><dayOfYear sur 3 chiffres>_<id_ark_court>.

11. Construction de la structure de sortie.
    - Une entree "items" par numero, chaque item contenant:
      - revue
      - parent_ark_date
      - year
      - day_of_year
      - numero_id
      - issue_ark
      - precision
      - status
      - error_stage
      - error_code
      - error_message

12. Ecriture des fichiers.
    - JSON final:
      - period {start_year, end_year}
      - total_issues
      - total_errors
      - total_events
      - items[]
    - CSV final (sans index explicite):
      - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision,status,error_stage,error_code,error_message

13. Trace d'execution console.
    - Affiche la revue en cours.
    - Affiche le nombre d'annees retenues.
    - Affiche le nombre final de fascicules exportes.

Commande d'execution type:
- .venv/bin/python -u scripts/pipeline_pdf/scraping_arks_numeros_gallica_pdf.py


ETAPE 2 - ARK de numero -> PDF Gallica (deja codee)
----------------------------------------------------
Script de reference:
- scripts/pipeline_pdf/scraping_pdf.py

Entree:
- input/arks_numeros.json (structure avec items[])

Sorties:
- JSON enrichi: input/arks_numeros.json
- CSV enrichi:  input/tableau_arks_numeros.csv
- PDFs sur disque:
  - pdf_process/<numero_id>/<numero_id>.pdf

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input (defaut: input/arks_numeros.json)
     - --output (defaut: input/arks_numeros.json)
     - --output-csv (defaut: input/tableau_arks_numeros.csv)
     - --pdf-root (defaut: pdf_process)
     - --requests-per-minute (defaut: 1)
     - --timeout-seconds (defaut: 30)
     - --user-agent
     - --cb-threshold (defaut: 5)
     - --cb-sleep-seconds (defaut: 600)
     - --force (re-telechargement des PDF existants)

2. Chargement du JSON d'entree et resolution de items[].
   - Lecture du payload JSON.
   - Verification de la presence de items[].

3. Initialisation HTTP robuste.
   - requests.Session + retry sur 429/5xx.
   - Rate limiter local (fenetre glissante 60s) a 1 req/min par defaut.

4. Iteration sur chaque item.
   - Lecture de numero_id, issue_ark, revue.
   - Validation minimale des champs obligatoires.
   - Normalisation de issue_ark vers "ark:/12148/...".

5. Construction de l'URL PDF et du chemin local.
   - URL PDF:
     - https://gallica.bnf.fr/<issue_ark>.pdf
   - Chemin local:
     - pdf_process/<numero_id_sanitized>/<numero_id_sanitized>.pdf
   - Ajout immediat dans item:
     - pdf_url
     - pdf_path

6. Strategie idempotente locale.
   - Si le PDF existe deja et taille > 0 et pas --force:
     - status = "ok"
     - skip du telechargement.

7. Telechargement du PDF si necessaire.
   - Attente rate limiter.
   - GET du PDF avec timeout.
   - raise_for_status puis ecriture binaire sur disque.
   - Mise a jour item:
     - pdf_size_bytes
     - status = "ok"
   - En cas d'erreur:
     - status = "error"
     - error_stage = "pdf_download" (ou stage pdf_*)
     - error_code = code structure (ex: 429, timeout, retry_error)
     - error_message = message erreur.

8. Ecriture des sorties enrichies.
   - JSON:
     - conserve items[]
     - ajoute/maj pdf_collection (downloaded/existing/errors/skipped_prior_error).
   - CSV:
     - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision,pdf_url,pdf_path,pdf_size_bytes,status,error_stage,error_code,error_message

Commande d'execution type:
- .venv/bin/python -u scripts/pipeline_pdf/scraping_pdf.py --input input/arks_numeros.json --output input/arks_numeros.json --output-csv input/tableau_arks_numeros.csv --pdf-root pdf_process


ETAPE 3 - PDF -> images bitonales (deja codee)
-----------------------------------------------
Script de reference:
- scripts/pipeline_pdf/scraping_pdf_to_images.py

Objectif:
- Convertir les PDF en images bitonales (PNG/TIFF) page par page.

Entree:
- input/arks_numeros.json (items enrichis avec pdf_path ou pdf_root)

Sorties:
- JSON enrichi: input/arks_numeros.json
- CSV enrichi: input/tableau_arks_numeros.csv
- Images:
  - images_process/<revue>/<numero_id>/page_0001.png (ou .tif)

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input, --output, --output-csv
     - --pdf-root (defaut: pdf_process)
     - --image-root (defaut: images_process)
     - --requests-per-minute (defaut: 120) [throttling local de conversion]
     - --cb-threshold (defaut: 5)
     - --cb-sleep-seconds (defaut: 600)
     - --dpi (defaut: 200)
     - --bitonal-threshold (defaut: 180)
     - --image-format (defaut: png, choix: png|tiff)
     - --first-page (defaut: 1)
     - --last-page (defaut: 0, 0 = derniere page)
     - --force

2. Resolution du PDF par item.
   - Priorite a item.pdf_path si present.
   - Sinon fallback: <pdf_root>/<numero_id>/<numero_id>.pdf.

3. Verification source PDF.
   - Controle presence du fichier PDF local et taille > 0.
   - En cas d'absence:
     - status = "error"
     - error_stage = "pdf_to_jpg_source"
     - error_code = "pdf_not_found".

4. Lecture metadata PDF.
   - Extraction du nombre de pages via pdfinfo_from_path.
   - Verification et normalisation de la plage de pages:
     - [first_page, last_page] bornee a total_pages.
   - Erreurs gerees:
     - pdfinfo_not_installed
     - pdf_page_count_error
     - pdf_syntax_error
     - invalid_page_range.

5. Conversion page par page.
   - Pour chaque page selectionnee:
     - rendu via pdf2image (fmt=ppm)
     - conversion en niveaux de gris
     - seuillage bitonal 1-bit selon bitonal-threshold
     - sauvegarde:
       - PNG optimise, ou
       - TIFF compression Group4.
   - Skip si image deja presente et taille > 0 (sauf --force).

6. Mise a jour des compteurs item.
   - images_total (pages selectionnees)
   - images_converted
   - images_existing
   - images_errors
   - image_output_dir

7. Gestion d'erreurs normalisee.
   - status = "error"
   - error_stage = pdf_to_jpg_input_validation|pdf_to_jpg_pdfinfo|pdf_to_jpg_convert|...
   - error_code / error_message.

8. Ecriture des sorties enrichies.
   - JSON:
     - conserve items[]
     - ajoute/maj pdf_image_collection (dpi/threshold/format + compteurs).
   - CSV:
     - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision,pdf_url,pdf_path,pdf_size_bytes,image_output_dir,images_total,images_converted,images_existing,images_errors,status,error_stage,error_code,error_message

Commande d'execution type:
- .venv/bin/python -u scripts/pipeline_pdf/scraping_pdf_to_images.py --input input/arks_numeros.json --output input/arks_numeros.json --output-csv input/tableau_arks_numeros.csv --pdf-root pdf_process --image-root images_process --image-format png --dpi 200 --bitonal-threshold 180


ETAPE 4 - Industrialisation / Orchestrateur (deja codee)
--------------------------------------------------------
Script de reference:
- scripts/pipeline_pdf/run_pipeline_gallica_pdf.py

Objectif:
- Enchainer automatiquement les 3 etapes avec reprise, idempotence et etat persistant.

Entrees:
- input/arks_revues.json
- input/arks_numeros.json
- input/tableau_arks_numeros.csv

Sorties:
- input/arks_numeros.json (mis a jour)
- input/tableau_arks_numeros.csv (mis a jour)
- manifest_iiif_process/state_pdf.json
- pdf_process/... et images_process/...

Sous-etapes detaillees:
1. Initialisation du run.
   - Charge l'etat state_pdf.json (ou l'initialise).
   - Ajoute une entree runs[] avec status="running".

2. Etape 1 conditionnelle (issues).
   - Detecte les revues a recalculer:
     - nouvelle revue,
     - ARK modifie,
     - revue non "done",
     - ou --force-step1.
   - Lance scraping_arks_numeros_gallica_pdf.py sur les revues pending seulement.
   - Merge intelligent du resultat dans input/arks_numeros.json.
   - Met a jour state.revues[*].status/last_error.

3. Etape 2 conditionnelle (PDF).
   - Detecte les items restants:
     - pdf_path manquant,
     - fichier absent,
     - fichier vide,
     - ou --force-pdf.
   - Lance scraping_pdf.py avec les parametres rpm/circuit-breaker/timeouts.

4. Etape 3 conditionnelle (images).
   - Detecte les items restants:
     - images_total==0,
     - images_errors>0,
     - converted+existing < total,
     - ou --force-images.
   - Lance scraping_pdf_to_images.py avec format/dpi/threshold/plage pages.

5. Finalisation etat.
   - Calcule status final du run: done/error selon return codes etapes.
   - Renseigne finished_at et last_run.
   - Sauvegarde state_pdf.json.

Configuration CLI principale:
- --issues-rpm (defaut: 10)
- --pdf-rpm (defaut: 1)
- --image-rpm (defaut: 120)
- --step1-cb-threshold / --step1-cb-sleep-seconds (5 / 600)
- --step2-cb-threshold / --step2-cb-sleep-seconds (5 / 600)
- --step3-cb-threshold / --step3-cb-sleep-seconds (5 / 600)
- --disable-step1 / --disable-step2 / --disable-step3
- --force-step1 / --force-pdf / --force-images

Commande d'execution type (pipeline complete):
- .venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py --revues-input input/arks_revues.json --numeros-json input/arks_numeros.json --numeros-csv input/tableau_arks_numeros.csv --pdf-root pdf_process --image-root images_process --image-format png --dpi 200 --bitonal-threshold 180
