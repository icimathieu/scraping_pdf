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

12. Ecriture des fichiers.
    - JSON final:
      - period {start_year, end_year}
      - total_issues
      - items[]
    - CSV final (sans index explicite):
      - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision

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
  - data_process/<revue>/<numero_id>.manifest.json

Sous-etapes detaillees:
1. Chargement de la configuration CLI.
   - Parametres principaux:
     - --input (defaut: input/arks_numeros.json)
     - --output (defaut: input/arks_numeros.json)
     - --output-csv (defaut: input/tableau_arks_numeros.csv)
     - --manifest-root (defaut: data_process)
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
     - data_process/<revue_sanitized>/<numero_id_sanitized>.manifest.json
   - Ajout immediat dans item:
     - manifest_url
     - manifest_path

6. Strategie idempotente locale.
   - Si le manifest existe deja et pas --force:
     - manifest_status = "exists"
     - skip du telechargement.

7. Telechargement du manifest si necessaire.
   - Attente rate limiter.
   - GET du manifest avec timeout.
   - raise_for_status + parsing JSON.
   - Ecriture du manifest sur disque.
   - Mise a jour item:
     - manifest_status = "downloaded"
   - En cas d'erreur:
     - manifest_status = "error"
     - manifest_error = message erreur.

8. Ecriture des sorties enrichies.
   - JSON:
     - conserve items[]
     - ajoute/maj manifest_collection (downloaded/existing/errors).
   - CSV:
     - revue,parent_ark_date,year,day_of_year,numero_id,issue_ark,precision,manifest_url,manifest_path,manifest_status,manifest_error

Commande d'execution type:
- .venv/bin/python -u scripts/scraping_manifest_gallica.py --input input/arks_numeros.json --output input/arks_numeros.json --output-csv input/tableau_arks_numeros.csv --manifest-root data_process


ETAPE 3 - manifest IIIF -> images bitonales full
------------------------------------------------
Objectif:
- Telecharger les pages image d'un numero, en bitonal full, puis passer au numero suivant.

Sous-etapes:
1. Charger les manifests recuperes a l'etape 2.
2. Pour chaque manifest, lister les canvases/pages dans l'ordre.
3. Pour chaque page:
   - extraire l'URL image source (resource.@id ou service.@id selon structure manifest).
4. Construire l'URL IIIF bitonale full pour la page.
   - Exemple de patron IIIF:
     - <base_iiif>/full/full/0/bitonal.jpg
   - Adapter exactement au endpoint Gallica accepte.
5. Telecharger page par page avec:
   - limite de debit conforme (5/min si endpoint full/full),
   - retries/backoff,
   - verif de taille/status.
6. Organiser les sorties disque:
   - dossier par numero_id,
   - nommage stable (ex: page_0001.jpg).
7. Ecrire un index de progression:
   - pages totales, pages ok, pages en erreur, date/heure, temps.
8. Ne passer au manifest suivant qu'une fois le precedent termine (ou marque termine avec erreurs).


ETAPE 4 - Industrialisation / automatisation
--------------------------------------------
Objectif:
- Orchestrer les 3 etapes avec reprise et idempotence.

Structure recommandee:
- step1_collect_issues.py
- step2_collect_manifests.py
- step3_download_images.py
- run_pipeline.py (orchestrateur)

Correspondance avec scripts actuels:
- step1_collect_issues.py -> scripts/scraping_arks_numeros_gallica.py
- step2_collect_manifests.py -> scripts/scraping_manifest_gallica.py
- step3_download_images.py -> scripts/scraping_images_gallica.py (a implementer)

Sous-etapes orchestrateur:
1. Lire input/arks_revues.json.
2. Lancer etape 1 seulement pour les revues non traitees.
3. Lancer etape 2 seulement pour les ARK-numeros sans manifest local.
4. Lancer etape 3 seulement pour les pages/images manquantes.
5. Produire un etat de progression persistant (ex: data_process/state.json).

Regles anti-retravail:
1. Idempotence par fichier de sortie:
   - si manifest existe et est valide, skip.
   - si image page existe et taille > 0, skip.
2. State store (JSON/SQLite) avec statuts:
   - revue: pending|done|error
   - issue_ark: manifest_done, images_done, last_error
   - timestamps updated_at
3. CLI orchestrateur:
   - --resume (par defaut)
   - --force (recalcul complet ou partiel)


Notes transverses
-----------------
- Toujours utiliser HTTPS.
- Garder un User-Agent explicite.
- Respecter strictement les limites de l'API Gallica et traiter 429 proprement.
- Eviter les chemins absolus dans le code; preferer des chemins relatifs configurables.
- Conserver des sorties reproductibles (JSON/CSV + logs).
