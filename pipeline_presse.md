# Pipeline presse généraliste — inventaire IIIF Gallica 1870-1914

Spécification de la collecte de l'**inventaire** des 18 titres de presse
généraliste numérisés par Gallica sur 1870-1914. Cette pipeline ne télécharge
**aucune image** : elle établit, numéro par numéro, ce qui existe réellement
chez Gallica, pour pouvoir ensuite cibler ce qu'on scrape.

Code : `scripts/pipeline_presse/`. Exploitation : `scripts/pipeline_presse/ops/`.

---

## 1. Pourquoi

Le corpus de presse déjà disponible (**Europeana Newspapers**, 18 titres,
187 666 exemplaires, OCR fourni) souffre de deux défauts :

1. **son OCR est de qualité médiocre** (OCR Europeana de 2015, hétérogène) ;
2. **il a des trous** — et pas des petits.

Mesuré le 2026-09-09 sur un échantillon de 6 couples (titre, année), en
comparant l'API `Issues` de Gallica au corpus Europeana local :

| titre | année | Gallica | Europeana | manquants |
|---|---|---:|---:|---:|
| le_temps | 1875 | 407 | 363 | **44** (10,8 %) |
| le_temps | 1895 | 380 | 363 | 17 (4,5 %) |
| le_petit_parisien | 1890 | 365 | **0** | **365** (100 %) |
| le_petit_parisien | 1910 | 364 | 365 | ~0 |
| la_croix | 1885 | 308 | 309 | ~0 |
| la_croix | 1905 | 307 | 53 | **254** (82,7 %) |
| **échantillon** | | **2 131** | **1 453** | **678 = 31,8 %** |

Des **années entières** manquent (`le_petit_parisien` 1890). Il faut donc un
inventaire de référence tiré de Gallica, pas d'Europeana.

⚠️ `le_temps` 1875 compte **407** numéros pour 365 jours : suppléments et
éditions multiples. **Ne jamais assimiler un numéro à une date.**

---

## 2. Les trois choix qui rendent la campagne possible

### 2.1 Les 18 ARK de périodique sont extraits HORS LIGNE

Chaque JSON Europeana porte les deux ARK Gallica : celui du **périodique**
(`cb…`, l'identité du titre) et celui de l'**exemplaire** (`bpt6k…`). Les 18 ARK
de titre ont donc été extraits du corpus local, **sans un seul appel à Gallica**.

C'est le seul usage d'Europeana dans cette pipeline : donner l'identité des
titres. Les numéros, eux, viennent tous de Gallica.

Sortie : `input/presse_titres.json` (18 entrées, un `cb` par titre — aucun titre
n'en a deux sur la période).

### 2.2 `Pagination` remplace les manifests IIIF — 146 jours → 5 jours

C'est le choix décisif. L'endpoint `manifest.json` de Gallica plafonne à
**1 req/min** (mesuré en mai 2026 sur les revues scientifiques) :

| voie | requêtes | durée |
|---|---:|---|
| Manifests IIIF classiques | ~211 000 | **146 jours** ⛔ |
| **`services/Pagination`** | ~211 000 | **~4,9 j** à 30 req/min |

`Pagination` renvoie `<nbVueImages>`, qui vaut **exactement** le nombre de canvas
du manifest (vérifié 4/4 sur le JORF), tient **40 req/min sans un seul 429**
(mesure du 2026-09-06) et répond en **0,1-0,2 s**. Comme l'URL du service image
de Gallica est déterministe (`/iiif/<ark>/f<n>`), on fabrique ensuite des
**manifests synthétiques** que `scraping_images_gallica.py` avale **sans
modification**.

Cadence retenue : **30 req/min**, la seule éprouvée à l'échelle (4 h 15 de
production sur le JORF, 0 erreur). 40/min gagnerait 1,2 jour mais n'a été testé
que sur 45 requêtes.

### 2.3 Le ledger JSONL rend le run reprenable

`presse_pagination.jsonl` est **écrit et flushé à chaque requête**. Un arrêt
(circuit breaker, reboot, coupure) ne coûte donc rien : la relance repart
exactement où on s'est arrêté. **Seuls les succès comptent comme connus** — un
ARK en erreur est retenté au run suivant.

---

## 3. Les étapes

    etape 1  services/Issues      ~600 req   ->  ~20 min   liste des numeros par titre/annee
    etape 2  services/Pagination  ~211k req  ->  ~4,9 j    nbVueImages par numero
    etape 3  construction         hors ligne ->  ~1 min    manifests + inventaires
    etape 4  archivage HF         20 commits ->  ~5 min

### Étapes 1 et 2 — `inventaire_presse.py`

```bash
python3 -u scripts/pipeline_presse/inventaire_presse.py \
    --titres input/presse_titres.json \
    --out-issues input/presse_fascicules.json \
    --out-pages input/presse_pagination.jsonl \
    --state-dir state/presse \
    --requests-per-minute 30
```

Options utiles : `--seulement <slug[,slug]>` pour un run partiel,
`--start-year` / `--end-year` (défaut 1870 / 1914).

### Étape 3 — `construire_entree_presse.py` (aucun réseau)

```bash
python3 -u scripts/pipeline_presse/construire_entree_presse.py
```

Produit :
- `input/presse_numeros.json` — l'entrée que le scraper d'images avalera le jour
  où on décidera quoi télécharger ;
- `manifests_presse/<titre>/<numero_id>.manifest.json` — les manifests synthétiques ;
- `input/inventaire_presse/<titre>.jsonl` — l'inventaire compact
  (`numero_id`, `ark`, `date`, `annee`, `pages`, `libelle`). **C'est celui-ci
  qu'on relira pour choisir les numéros**, pas les manifests.

`numero_id` = `<titre>_<AAAAMMJJ>_<ark court>`. L'ARK y figure parce que
**plusieurs numéros peuvent partager une date** (§1) : la date seule ne suffit
pas à identifier un numéro.

⚠️ Le champ `manifest_path` n'est **jamais** écrit dans les items : il
court-circuiterait `--manifest-root` (piège documenté en `CLAUDE.md` §15).

### Étape 4 — `hf_backup_manifests_presse.py`

```bash
python3 -u scripts/pipeline_presse/hf_backup_manifests_presse.py
```

Pousse dans le dataset privé `icimathieu/corpus-memoire-ocr` :

    manifests_presse/<titre>.tar.zst        1 commit par titre (18)
    manifests_presse/inventaires.tar.zst    les 18 JSONL compacts
    manifests_presse/README.md

**20 commits au total**, très loin de la limite HF de **128 commits/heure**.
Ne jamais descendre à la granularité du fichier : 211 000 manifests =
211 000 commits. Ledger local `state/hf_manifests_presse.txt` (empreinte
nb fichiers + octets par titre) → un titre inchangé est sauté. Backoff sur 429.

Auth : token lu depuis `~/.cache/huggingface/token`, jamais en variable d'env.

---

## 4. Garde-fous 429 et surveillance

### Circuit breaker à trois niveaux (dans `inventaire_presse.py`)

| déclencheur | réaction |
|---|---|
| **429** | cooldown **long et immédiat** (900 s, ou `Retry-After` si plus grand) |
| N échecs transitoires consécutifs (5xx, timeout) — défaut 8 | cooldown court (300 s) |
| au-delà de 6 cooldowns | **arrêt net** — le wrapper relancera dans 30 min |

🔑 **Le 429 est volontairement absent du `status_forcelist` d'urllib3.** S'il y
figurait, la bibliothèque le retenterait toute seule et **le throttling de
Gallica deviendrait invisible** : ni compté, ni cooldowné. On lui laisse les
5xx, qui sont du bruit réseau sans signification.

### Wrapper `run_presse.sh`

Relance en boucle après un arrêt du circuit breaker (`RESTART_SLEEP=1800`).
Pre-flight : `/data` monté **et** entrée présente — sans quoi on écrirait sur le
SSD racine sans s'en apercevoir. Pose `DONE` **seulement** si `faits >= total`.

### Watchdog `monitor_presse.sh` (cron `*/15` + `@reboot`)

Deux rôles distincts :
1. **relance** si la session tmux est morte ;
2. **détection de stagnation** — il juge sur ce qui est **produit**, jamais sur
   la présence d'un process. 3 ticks sans une seule pagination = 45 min, alors
   qu'on en attend ~1 350 → notif `🔴 BLOQUÉ` avec les compteurs 429 / erreurs /
   cooldowns. **Une seule alerte par épisode** (fichier témoin).

Garde : pendant la phase 1 (énumération, ~20 min) le ledger est vide par
construction — la mesure de stagnation est suspendue tant que
`presse_fascicules.json` n'existe pas.

### Digest quotidien `daily_report_presse.sh` — 9h10 Paris

Numéros paginés / total, delta 24 h, ETA, **429 / erreurs / cooldowns**, poids
du ledger, disque. Garde de stagnation : `+0 numéro en 24 h` avec session active
→ `🔴 ANOMALIE`, jamais `Etat: OK`.

Cron DST-safe (le cron Debian n'honore pas `TZ` par utilisateur) :

    10 7,8 * * * [ "$(TZ=Europe/Paris date +\%H)" = 09 ] && .../daily_report_presse.sh

9h10 et non 9h00 : rog a déjà son digest OCR IIIF à 8h55.

🔑 **Numérateur et dénominateur ne viennent d'aucun fichier écrit en fin de
run** — le numérateur est compté sur le ledger (écrit en continu), le
dénominateur sur l'énumération. C'est la règle du `CLAUDE.md` §22, née d'un
digest qui annonçait `0 / 100042` pendant toute une campagne de 17 jours.

---

## 5. Exploitation sur rog-server

    Code      ~/code_memoire/scraping_presse/scripts/pipeline_presse/
    Donnees   /data/corpus_memoire/2026_presse_inventaire/{state,logs}
    Ops       ~/ops_presse/{run,monitor,daily_report,notify}_presse.sh
    venv      ~/code_memoire/scraping_gallica/.venv  (requests 2.34.2, py 3.14.4)
    tmux      presse
    ntfy      topic de rog (~/ops_presse/ntfy_topic.txt, jamais versionne)

Le job est **purement réseau** (0,2 s par requête, aucun CPU, aucun GPU) et
tourne en `nice -n 10` : il ne gêne ni l'OCR GPU ni les autres campagnes de rog.

⚠️ Les chemins de code et de venv sont **en dur dans le wrapper**, pas passés
par variable d'environnement : le watchdog le relance depuis cron, sans
environnement (leçon `CLAUDE.md` §13).

⚠️ `tmux has-session -t presse` matche par **préfixe** — d'où `-t =presse`
(matching exact) partout dans les scripts d'ops (bug du 2026-09-06).
