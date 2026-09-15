Pipeline JORF 1870-1880 (3 etapes)
===================================

Objectif
--------
Collecter les images du *Journal officiel de la Republique francaise*
(ark:/12148/cb328020909) pour la periode **05/09/1870 - 31/12/1880**, qui
contient les debats de l'Assemblee nationale, de la Chambre des deputes, du
Senat et du Gouvernement de la Defense nationale avant leur separation en
series distinctes en 1881.

Cette periode complete par le bas le corpus JORF deja oceirise (Chambre et
Senat a partir de 1881, 312 202 pages) : ce sont les 11 annees manquantes cote
Chambre / Assemblee nationale et les 5 annees manquantes cote Senat.

Sur cette periode, **Chambre et Senat ne sont pas separes** : les debats sont
publies dans le fascicule quotidien, meles aux decrets, rapports et annonces.
Le tri par chambre et par type de contenu se fera a la structuration post-OCR,
pas au scraping.


Ce qui distingue cette pipeline de la pipeline IIIF des revues
--------------------------------------------------------------

**1. Pas de scraping des manifests IIIF.**
L'endpoint manifest ne tolere qu'~1 req/min (pipeline_manifest.md), soit plus de
5 jours pour 7 611 fascicules. On le remplace par le service `Pagination`, qui :

- renvoie `nbVueImages` ;
- tient **40 req/min sans un seul 429** (mesure du 2026-09-06, 45 requetes) ;
- donne exactement le nombre de canvas du manifest (verifie 4/4 sur des
  fascicules de 4, 16, 60 et 80 pages).

Comme l'URL du service image IIIF de Gallica est deterministe
(`https://gallica.bnf.fr/iiif/<ark>/f<n>`), on reconstruit des **manifests
synthetiques** que `scraping_images_gallica.py` consomme sans modification.
Gain : ~4 h au lieu de ~5 jours.

**2. Deduplication par date.**
L'API Issues renvoie **7 611 ARK pour 3 708 dates** : Gallica publie 2 a 4
numerisations du meme fascicule. Ce sont bien des doublons, pas des editions
differentes -- verifie le 2026-09-06 sur le 15/10/1870, dont les 3 ARK portent
le meme numero (A2,N284), le meme titre, le meme editeur et la meme cote BnF
(2010-217349) ; et par comptage de pages, identique sur 13 paires sur 14.

**3. Format JPEG, pas PNG gris.**
Une page de journal est du texte dense sur six colonnes : elle compresse mal en
PNG. Mesure sur 6 pages reelles :

    cote long  format     Mo/page   corpus complet
         3600  PNG gris      5,14        502 Go
         3600  JPEG          1,66        162 Go
         2864  JPEG          1,18        115 Go   <- retenu

Le format retenu (`!2864,2864`, `native.jpg`, stocke tel quel) est **exactement
celui du corpus JORF 1881+ deja oceirise** (2048x2864, JPEG q80, ~1 Mo/page) :
c'est le seul dont la validite OCR soit prouvee sur ce corpus precis. Gallica le
sert deja encode, donc **aucun re-encodage local** (0 CPU) et un download 8,5x
plus leger qu'en PNG pleine resolution.


ETAPE 1 - Inventaire
--------------------
Script : `scripts/pipeline_jorf/inventaire_jorf.py`

Deux phases :
1. **Enumeration** via le service `Issues` (annees, puis fascicules par annee).
   ~12 requetes.
2. **Pagination** via le service `Pagination`, un appel par ARK.

Sorties :
- `input/jorf_fascicules.json` -- liste brute des ARK (ark, annee, libelle, date)
- `input/jorf_pagination.jsonl` -- ledger `{ark, annee, date, pages}`

Parametres principaux (defauts) :
- `--ark`                  ark:/12148/cb328020909/date
- `--start-year`           1870
- `--end-year`             1880
- `--requests-per-minute`  30 (mesure a 40 sans 429, marge gardee)
- `--echecs-consecutifs-max` 15

La phase 2 est **reprenable** : chaque reponse est ecrite au fil de l'eau dans le
JSONL, relu au demarrage. Seuls les succes comptent comme connus, donc un ARK en
erreur est automatiquement retente au run suivant. Une ligne corrompue par un
run tue en pleine ecriture est ignoree plutot que de casser la reprise.

Duree mesuree : ~4 h pour 7 611 ARK a 30 req/min.


ETAPE 2 - Deduplication et manifests synthetiques
--------------------------------------------------
Script : `scripts/pipeline_jorf/construire_entree_jorf.py`

Regle de selection, un ARK par date :
1. le plus de pages (jamais retenir une numerisation tronquee) ;
2. a egalite, la famille `bpt6k209*` (presente sur 3 693 des 3 708 dates) ;
3. puis l'ordre alphabetique, pour que le choix soit reproductible.

Les numerisations concurrentes sont conservees dans le champ `arks_alternatifs`
de chaque item : elles servent de filet a l'etape 3.

Sorties :
- `input/jorf_numeros.json` -- items consommes par `scraping_images_gallica.py`
- `<manifest-root>/jorf/<numero_id>.manifest.json` -- manifests synthetiques

`numero_id` = `jorf<AAAAMMJJ>_<ark_court>`, l'ARK etant conserve dans
l'identifiant puisque plusieurs numerisations coexistent pour la meme date.

Le script signale (sans les exclure) les **outliers de pagination**, c'est-a-dire
les fascicules depassant 4x la mediane de leur annee : ce sont typiquement les
tables annuelles, que le mémoire ne veut pas. Exemple repere : `bpt6k2093191z`,
date du 01/01/1871, **60 pages** quand les fascicules voisins en font 2,
reference `(A3,N0)` -- un "numero 0" de 60 pages est une table. Les verifier via
`services/OAIRecord`, puis les lister dans `--exclure`.

Sont aussi ecartes d'office les fascicules dont le libelle ne porte que l'annee
(pas de date de jour) : meme profil de tables.

/!\ Le champ `manifest_path` n'est **jamais** ecrit dans les items : il
court-circuiterait `--manifest-root` (piege CLAUDE.md §15).


ETAPE 3 - Telechargement des images
------------------------------------
Script : `scripts/pipeline_manifest_iiif/scraping_images_gallica.py`, **inchange**,
pilote par le wrapper `~/memoire/ops/run_jorf.sh` sur antec-server.

    --quality native --format jpg --iiif-size '!2864,2864' --no-grayscale
    --requests-per-minute 4

`--no-grayscale` est essentiel : Gallica sert deja un JPEG a la bonne taille, la
normalisation Pillow ne ferait que le re-encoder pour rien.

Cadence 4 img/min : la BnF documente 5/min, et le benchmark maison mesure 0 % de
429 a 4/min contre 28 % a 6/min. Ne pas monter a 5 (CLAUDE.md §8, §20).

Reprise : elle est **basee sur les fichiers presents sur disque**
(`page_NNNN.jpg` non vide), pas sur un etat central. Corollaire : toute
suppression locale d'images doit s'accompagner du retrait des numeros
correspondants du manifeste, sinon la relance suivante les retelecharge
integralement (piege CLAUDE.md §20, 05/09).


ETAPE 3 bis - Reparation (optionnelle)
---------------------------------------
Script : `scripts/pipeline_jorf/reparer_jorf.py`

A lancer **seulement apres convergence** du scrape. Pour chaque numero
incomplet, regenere le manifest depuis une numerisation alternative et relance
le scraper sur le **meme `numero_id`** : les pages presentes sont sautees, seules
les manquantes sont tirees de l'autre exemplaire.

Garde-fou : on ne repare que depuis une alternative ayant **exactement le meme
nombre de pages**, sans quoi rien ne garantit que `fN` designe la meme page dans
les deux numerisations. Les cas non alignes sont listes pour arbitrage manuel.


/!\ Gallica throttle en 404, pas en 429
----------------------------------------
Piege majeur, tombe en plein pendant la mise au point de cette pipeline le
2026-09-06 : une rafale de sondes IIIF (~40 requetes en 15 min, largement
au-dessus des 4/min) a rendu **404 les dernieres vues de 8 ARK sur 8**. J'en ai
conclu qu'une famille de numerisation etait tronquee. **C'etait faux** : apres
10 min de pause, toutes les sondes rejouees repondent 200, y compris celle qui
avait ete declaree "trou permanent" apres une sonde pretendument isolee -- elle
suivait la rafale de trop pres.

Consequences :
- **Ne jamais conclure a un trou de corpus sur une sonde rapprochee.** Un trou
  n'est reel qu'apres convergence du scraper a 4 img/min.
- L'architecture 2 passes + garde de convergence de `run_jorf.sh` rattrape
  d'elle-meme les 404 transitoires : une page non ecrite est retentee a la
  passe suivante (reprise disque). C'est precisement sa raison d'etre.


Volumetrie
----------
    ARK renvoyes par l'API Issues        7 611
    Dates distinctes (= numeros)         3 708
    Pages estimees                     ~97 600   (echantillon stratifie, 330 numeros)
    Poids a 1,18 Mo/page                ~115 Go
    Duree a 4 img/min, 1 machine         ~17 j

Repartition des pages par annee (echantillon) : 2,4 p./numero en 1870 (penurie
de papier pendant le siege de Paris, verifie sur 10 dates et 3 numerisations
concordantes), ~24 p. de 1872 a 1874, ~35 p. de 1878 a 1880.


Exploitation sur imac-server (depuis le 08/09/2026)
---------------------------------------------------
    Code      ~/code_memoire/scraping_pdf/scripts/pipeline_jorf/
    Donnees   /data/corpus_memoire/2026_JORF_1870_1880/{images,manifests,state,logs}
    Ops       ~/ops/{run_jorf,monitor_jorf,daily_report_jorf,notify_jorf}.sh
              ~/ops/state_summary_jorf.py

La campagne a demarre sur antec-server le 06/09 et a ete rebasculee sur
imac-server le 08/09 (coupure de courant longue chez l'hebergeur d'antec).

🔑 **Une campagne batie sur des manifests synthetiques se deplace hors ligne.**
Il a suffi de repousser les 4 fichiers d'inventaire et de rejouer
`construire_entree_jorf.py` : 3 700 manifests regeneres en 1 seconde, zero appel
a Gallica. Par la voie manifest classique, la bascule aurait coute 5 jours.

- `run_jorf.sh` : pre-flight (manifests presents **et** plancher disque 15 Go),
  puis boucle de relance. Pose `DONE` uniquement a 0 page manquante, ou apres
  convergence (deux passes donnant le meme residuel), jamais sur un simple
  `exit 0`.
- `monitor_jorf.sh` (cron `*/15` + `@reboot`) : relance la session tmux si elle
  meurt, **et** detecte la stagnation (3 ticks sans une image = 45 min alors
  qu'on en attend ~180). Une seule alerte par episode.
- `daily_report_jorf.sh` : digest ntfy a 9h00 Europe/Paris, cron DST-safe
  (`0 7,8 * * * [ "$(TZ=Europe/Paris date +\%H)" = 09 ] && ...`). Signale
  `ANOMALIE` si la session tourne avec +0 page en 24 h -- "tourner" n'est pas
  "produire".
- `state_summary_jorf.py` : denominateur du manifeste, numerateur du disque.
  Jamais les deux d'un fichier d'etat ecrit en fin de course (CLAUDE.md §20).


Enchainement autonome
---------------------
Les trois etapes s'enchainent sans intervention humaine :

    tmux jorf_inv   inventaire_puis_entree.sh   etapes 1 + 2 (~4 h)
         |
    cron */10       armer_scrape_jorf.sh        arme quand les faits sont reunis
         |
    tmux jorf       run_jorf.sh                 etape 3 (~17 j)

`armer_scrape_jorf.sh` **constate des faits** plutot que de se fier a une heure
annoncee (meme logique que `arm_backup_purge.sh` sur rog) :

1. la session `jorf_inv` n'existe plus ;
2. `input/jorf_numeros.json` existe et contient >= 3 000 items coherents ;
3. autant de manifests ecrits que d'items ;
4. disque suffisant : `pages x 1,18 Mo` + 15 Go de marge.

Alors seulement il installe les crons de surveillance, lance `tmux jorf`,
notifie par ntfy, et **se retire lui-meme du crontab**. S'il refuse pour cause
de disque, il notifie et se retire aussi -- il ne boucle jamais en silence.

C'est un script separe, pas une 3e etape de `inventaire_puis_entree.sh` : ce
dernier tournait deja quand la decision a ete prise, et **bash relit un script
en cours d'execution par offset** -- l'editer en place l'aurait fait derailler
(piege CLAUDE.md §21, 02/09). Un armeur externe survit en prime a un reboot.


/!\ `tmux has-session -t <nom>` matche par PREFIXE
---------------------------------------------------
Bug reel, attrape au test a blanc du 2026-09-06 : `tmux has-session -t jorf`
repond **vrai** quand seule la session `jorf_inv` existe. L'armeur en a conclu
que le scrape tournait deja et **s'est retire du crontab aussitot**, ce qui
aurait laisse la campagne sans jamais demarrer.

Le meme defaut etait present dans `monitor_jorf.sh` (le watchdog aurait cru le
scrape vivant tant que l'inventaire tournait) et dans `daily_report_jorf.sh`.

Correctif : toujours `-t =jorf` (matching exact, tmux >= 2.x). Verifie sur
tmux 3.6 : `-t jorf` -> OUI, `-t =jorf` -> NON, `-t =jorf_inv` -> OUI.

Regle generale pour la flotte : soit le matching exact `=`, soit des noms de
session dont aucun n'est le prefixe d'un autre.
