# Conventions de commande pour le projet

## Règle : commandes minimales

**Ne jamais préfixer les commandes par `caffeinate -i`** : l'orchestrateur le lance lui-même au démarrage (subprocess lié au PID parent, meurt à la fin du run). Donner les commandes telles quelles.

Tous les autres défauts sont déjà baked in dans les scripts `scripts/pipeline_pdf/*.py` :

| Option | Défaut |
|---|---|
| `--start-year` / `--end-year` | 1870 / 1914 |
| `--issues-rpm` (étape 1) | 5 |
| `--pdf-rpm` (étape 2) | **0.3** (1 PDF / 3 min 20s, baisse pour eviter throttling Gallica sur gros PDFs) |
| `--image-rpm` (étape 3) | 30 |
| `--timeout-pdf` (étape 2) | **1800s** (30 min, pour les gros PDFs ~200 MB) |
| `--step2-stalled-timeout-seconds` | **120s** (abandon si .part ne grossit plus depuis 2 min = connexion coupee) |
| `--step1-cb-threshold` / `--step3-cb-threshold` | 5 |
| `--step2-cb-threshold` | **3** (plus strict, moins tolerant aux echecs) |
| `--step{1,2,3}-cb-sleep-seconds` | 600 |
| `--step1-cb-max-cooldowns` / `--step3-cb-max-cooldowns` | 3 |
| `--step2-cb-max-cooldowns` | **2** (plus strict) |
| `--step2-cookies-file` | `gallica.bnf.fr_cookies.txt` (auto si present) |
| `--user-agent` | **vide** (Firefox envoie son UA natif — important pour eviter les mismatch ALTCHA quand les cookies sont generes depuis le meme Firefox) |
| `caffeinate -i` | **automatique** sur macOS (opt-out `--no-caffeinate`) |

⇒ **Ne pas réécrire ces valeurs dans la commande**.

## Modèle minimal de commande à donner à l'utilisateur

### Étape 1 (issues)
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step2 --disable-step3
```

### Étape 2 (PDF Selenium)
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step3
```

### Étape 3 (images)
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --disable-step1 --disable-step2 \
  --delete-pdf-after-success
```

### Pipeline complète (1+2+3)
```bash
.venv/bin/python -u scripts/pipeline_pdf/run_pipeline_gallica_pdf.py \
  --delete-pdf-after-success
```

## Opt-out

- `--no-caffeinate` : désactive le caffeinate automatique (utile pour debug ou hors macOS).
- Override possible des cb-max-cooldowns avec `--step{1,2,3}-cb-max-cooldowns N`.

## Notes

- `caffeinate -i` est lancé en subprocess lié au PID parent (Popen `-w`) → meurt automatiquement à la fin du run, pas besoin de cleanup manuel.
- Les chemins par défaut (`--revues-input input/arks_revues.json`, etc.) sont bons → ne pas les répéter sauf besoin spécifique.
