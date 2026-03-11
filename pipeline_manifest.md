Pipeline Gallica (4 etapes)
===========================

Objectif global
---------------
- A partir d'ARK de revues, produire la liste complete des ARK de numeros sur une periode donnee.
- Pour chaque numero, recuperer le manifest IIIF.
- A partir du manifest, telecharger les images bitonales "full" page par page.
- Industrialiser l'ensemble dans un orchestrateur avec reprise.


ETAPE 1 - Revue ARK -> ARK de tous les numeros (deja codee)
-----------------------------------------------------------
Script de reference:
- scripts/scraping_arks_numeros_gallica.py

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
- .venv/bin/python -u scripts/scraping_arks_numeros_gallica.py


ETAPE 2 - ARK de numero -> manifest.json IIIF (deja codee)
----------------------------------------------------------
Script de reference:
- scripts/scraping_manifest_gallica.py

Entree:
- input/arks_numeros.json (structure avec items[])

Sorties:
- JSON enrichi: input/arks_numeros.json
- CSV enrichi:  input/tableau_arks_numeros.csv
- Manifests sur disque:
  - manifest_iiif_process/<revue>/<numero_id>.manifest.json

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input (defaut: input/arks_numeros.json)
     - --output (defaut: input/arks_numeros.json)
     - --output-csv (defaut: input/tableau_arks_numeros.csv)
     - --manifest-root (defaut: manifest_iiif_process)
     - --requests-per-minute (defaut: 5)
     - --timeout-seconds (defaut: 15)
     - --user-agent
     - --force (re-telechargement des manifests existants)

2. Chargement du JSON d'entree et resolution de items[].
   - Cas standard: items[] existe deja.
   - Cas legacy: reconstruction de items[] depuis listes numeros/urls si present.

3. Initialisation HTTP robuste.
   - requests.Session + retry sur 429/5xx.
   - Rate limiter local (fenetre glissante 60s) a 5 req/min par defaut.

4. Iteration sur chaque item.
   - Lecture de numero_id, issue_ark, revue.
   - Validation minimale des champs obligatoires.
   - Normalisation de issue_ark vers "ark:/12148/...".

5. Construction des chemins/URLs manifest.
   - URL:
     - https://gallica.bnf.fr/iiif/<issue_ark>/manifest.json
   - Chemin local:
     - manifest_iiif_process/<revue_sanitized>/<numero_id_sanitized>.manifest.json
   - Ajout immediat dans item:
     - manifest_url
     - manifest_path

6. Strategie idempotente locale.
   - Si le manifest existe deja et pas --force:
     - status = "ok"
     - skip du telechargement.

7. Telechargement du manifest si necessaire.
   - Attente rate limiter.
   - GET du manifest avec timeout.
   - raise_for_status + parsing JSON.
   - Ecriture du manifest sur disque.
   - Mise a jour item:
     - status = "ok"
   - En cas d'erreur:
     - status = "error"
     - error_stage = "manifest_download" (ou autre stage manifest_*)
     - error_code = code structure (ex: 429, timeout, value_error)
     - error_message = message erreur.

8. Ecriture des sorties enrichies.
   - JSON:
     - conserve items[]
     - ajoute/maj manifest_collection (downloaded/existing/errors).
   - CSV:
     - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision,manifest_url,manifest_path,status,error_stage,error_code,error_message

Commande d'execution type:
- .venv/bin/python -u scripts/scraping_manifest_gallica.py --input input/arks_numeros.json --output input/arks_numeros.json --output-csv input/tableau_arks_numeros.csv --manifest-root manifest_iiif_process


ETAPE 3 - manifest IIIF -> images bitonales full (deja codee)
-------------------------------------------------------------
Script de reference:
- scripts/scraping_images_gallica.py

Objectif:
- Telecharger les pages image d'un numero, en bitonal full, puis passer au numero suivant.

Entree:
- input/arks_numeros.json (items enrichis avec issue_ark + manifest_path ou manifest_root)

Sorties:
- JSON enrichi: input/arks_numeros.json
- CSV enrichi: input/tableau_arks_numeros.csv
- Images:
  - images_process/<revue>/<numero_id>/page_0001.jpg ...

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input, --output, --output-csv
     - --manifest-root (defaut: manifest_iiif_process)
     - --image-root (defaut: images_process)
     - --requests-per-minute (defaut: 5)
     - --timeout-seconds
     - --quality (defaut: bitonal)
     - --format (defaut: jpg)
     - --max-pages (0 = toutes)
     - --force

2. Resolution du manifest par item.
   - Priorite a item.manifest_path si present.
   - Sinon fallback: <manifest_root>/<revue>/<numero_id>.manifest.json.

3. Chargement/parsing du manifest.
   - Lecture JSON locale.
   - Verification sequences/canvases.
   - En cas d'erreur:
     - status = "error"
     - error_stage = images_manifest_path|images_manifest_load|images_manifest_parse
     - error_code / error_message renseignes.

4. Construction des URLs image par page.
   - Extraction de service.@id (ou fallback resource.@id).
   - Construction URL:
     - <service_id>/full/full/0/bitonal.jpg

5. Telechargement page par page.
   - Rate limit 5 req/min.
   - Retry/backoff sur 429/5xx.
   - Skip si fichier existe et taille > 0 (sauf --force).
   - Ecriture dans images_process/<revue>/<numero_id>/page_XXXX.jpg.

6. Mise a jour des compteurs item.
   - images_total
   - images_downloaded
   - images_existing
   - images_errors
   - image_output_dir

7. Normalisation d'erreurs (homogene avec etapes 1 et 2).
   - status = ok|error
   - error_stage
   - error_code
   - error_message

8. Ecriture finale.
   - JSON: maj items + images_collection + totaux.
   - CSV: colonnes metier + manifest + images + status/error_*.


ETAPE 4 - Industrialisation / automatisation
--------------------------------------------
Objectif:
- Orchestrer les 3 etapes avec reprise et idempotence.

Script de reference:
- scripts/run_pipeline_gallica.py

Entrees principales:
- input/arks_revues.json
- input/arks_numeros.json
- input/tableau_arks_numeros.csv

Sorties principales:
- manifest_iiif_process/state.json
- input/arks_numeros.json (mis a jour a chaque etape)
- input/tableau_arks_numeros.csv (mis a jour a chaque etape)
- manifest_iiif_process/<revue>/<numero_id>.manifest.json
- images_process/<revue>/<numero_id>/page_XXXX.jpg

Sous-etapes detaillees:
1. Charger l'etat de pipeline.
   - Lecture de manifest_iiif_process/state.json.
   - Initialisation si absent:
     - revues {}
     - runs []

2. Determiner les revues a retraiter pour l'etape 1.
   - Pour chaque revue de input/arks_revues.json:
     - traiter si revue absente de state
     - ou statut precedent != done
     - ou ARK modifie
     - ou --force-step1.

3. Executer l'etape 1 uniquement pour les revues en attente.
   - Generation d'un JSON temporaire avec les seules revues a traiter.
   - Appel de scripts/scraping_arks_numeros_gallica.py.
   - Merge des items produits avec input/arks_numeros.json existant:
     - remplacement uniquement pour les revues retraitees.
   - Mise a jour de state["revues"][revue] avec:
     - status done|error
     - ark
     - updated_at
     - last_error

4. Executer l'etape 2 seulement si necessaire.
   - Comptage des manifests manquants.
   - Si aucun manifest manquant (et pas --force-manifests): skip.
   - Sinon appel de scripts/scraping_manifest_gallica.py.

5. Executer l'etape 3 seulement si necessaire.
   - Comptage des numeros encore incomplets en images.
   - Si aucun numero restant (et pas --force-images): skip.
   - Sinon appel de scripts/scraping_images_gallica.py.

6. Journalisation et reprise.
   - Chaque execution est enregistree dans state["runs"] avec:
     - started_at
     - finished_at
     - step1_rc / step2_rc / step3_rc
     - status final done|error
   - Tous les outputs des scripts sont streames en console.

Regles anti-repetition effectivement implementees:
1. Etape 1:
   - ne relance pas les revues deja done avec meme ARK (sauf --force-step1).
2. Etape 2:
   - skip si manifest deja present (sauf --force-manifests).
3. Etape 3:
   - skip si images deja completes (sauf --force-images).
   - dans le script image, skip fichier page deja present et non vide (sauf --force).

Parametres CLI utiles de l'orchestrateur:
- --revues-input
- --numeros-json
- --numeros-csv
- --manifest-root
- --image-root
- --state-file
- --issues-rpm (defaut 10)
- --manifest-rpm (defaut 5)
- --image-rpm (defaut 5)
- --force-step1
- --force-manifests
- --force-images
- --disable-step1 / --disable-step2 / --disable-step3

Commande d'execution type:
- .venv/bin/python -u scripts/run_pipeline_gallica.py --revues-input input/arks_revues.json --numeros-json input/arks_numeros.json --numeros-csv input/tableau_arks_numeros.csv --manifest-root manifest_iiif_process --image-root images_process


Notes transverses
-----------------
- Toujours utiliser HTTPS.
- Garder un User-Agent explicite.
- Respecter strictement les limites de l'API Gallica et traiter 429 proprement.
- Eviter les chemins absolus dans le code; preferer des chemins relatifs configurables.
- Conserver des sorties reproductibles (JSON/CSV + logs).
