# Translation difficulty scorer

Estimates migration effort for Domo Magic ETL dataflows by translating each flow to **Snowflake** SQL (via `argo-utils-cli`’s `DataflowTranslator`) and scoring every step: **type base minutes** + **SQL length add-on** (1 point ≈ 1 minute). **`SelectValues` is always 1 point** and has no length surcharge.

## Setup

1. Install this repo’s base deps (pandas, PyYAML, google API clients, `python-dotenv`, etc.):

   ```bash
   pip install -r requirements.txt
   ```

2. Install **argo-utils-cli** in the same environment (sibling folder layout):

   ```bash
   pip install -e ../argo-utils-cli
   ```

   Or use [requirements-translation-difficulty.txt](../requirements-translation-difficulty.txt).

3. Environment variables:

   | Variable | Purpose |
   |----------|---------|
   | `DOMO_DEVELOPER_TOKEN` | Domo developer token |
   | `DOMO_INSTANCE` | Domo hostname / instance id |
   | `GOOGLE_SHEETS_CREDENTIALS_FILE` | Service account JSON (Sheets API) |
   | `MIGRATION_SPREADSHEET_ID` | Target spreadsheet id |
   | `TRANSLATION_DIFFICULTY_OUTPUT_SHEET` | (optional) Main tab name, default `CTE Points Analysis` |
   | `TRANSLATION_DIFFICULTY_DETAIL_SHEET` | (optional) Detail tab name |
   | `INTERMEDIATE_MODELS_SHEET_NAME` | (optional) Inventory tab for `export-inventory`, default `Inventory` |
   | `TRANSLATION_DIFFICULTY_MDAAS_TASKS_SHEET` | (optional) Tab whose `Dataflow ID` rows are **excluded** from scoring (default `MDAAS Tasks`). If the tab is missing or empty, no exclusions are applied. |

   Optional `.env` in the project root is loaded automatically.

### MDAAS Tasks exclusions

For `score`, any **Dataflow ID** listed on the **MDAAS Tasks** tab (same spreadsheet) is **skipped**, so those flows are not analyzed. The tab must include a **`Dataflow ID`** column (header match is case-insensitive). If the tab does not exist or has no usable column, the run continues and scores everything else.

- Disable: `--no-mdaas-exclude`
- Custom tab name: `--mdaas-tasks-sheet "Other Tab"` or set `TRANSLATION_DIFFICULTY_MDAAS_TASKS_SHEET`.

Filtering order: build ID list → remove MDAAS-listed IDs → `--max-dataflows` → `--skip-existing`.

## Usage

From the **Domo-to-snowflake-migration** repository root:

```bash
# Via unified migration CLI (same behavior; forwards arguments)
python main.py weighting export-inventory
python main.py weighting score --from-sheet Inventory --max-dataflows 10

# Canonical module path (implementation under tools/utils/)
python -m tools.utils.translation_difficulty export-inventory
python -m tools.utils.translation_difficulty score --from-sheet Inventory
python -m tools.utils.translation_difficulty score --ids 144 4927
python -m tools.utils.translation_difficulty score --from-api-list
python -m tools.utils.translation_difficulty score --from-sheet Inventory --max-dataflows 20 --skip-existing
```

Legacy alias (same code):

```bash
python -m tools.translation_difficulty export-inventory
```

Override spreadsheet or credentials:

```bash
python -m tools.utils.translation_difficulty --spreadsheet-id YOUR_ID --credentials path/to/sa.json score --ids 1
```

Alternate entry (adds repo root to `sys.path`):

```bash
python tools/scripts/run_translation_difficulty.py export-inventory
```

## Output

- **Main sheet** (default `CTE Points Analysis`): columns match the migration CSV — `filename`, `Dataflow ID`, `Step Points`, `Total Tiles`, `Subtotal Points`, `Total Points`. **Total Points is left blank** for manual adjustment.
- **Subtotal Points** = `Step Points` + `Total Tiles` (same convention as numeric rows in the reference inventory).
- **Detail sheet** (default `Translation difficulty detail`): one row per step with `base_minutes`, `length_minutes`, `sql_chars`, `success`.

Failed dataflows still produce a main row with empty numeric fields; check logs for errors.

## Tuning scores

Edit [tools/utils/translation_difficulty/weights.yaml](../tools/utils/translation_difficulty/weights.yaml):

- `type_base_minutes`: per Domo action type (e.g. `ExpressionEvaluator`, `MergeJoin`).
- `defaults`: `bucket_chars`, `minutes_per_bucket`, `length_addon_cap`, unknown-type default, and fallback sizing when SQL render is empty.

`SelectValues` is fixed in code and is not controlled by YAML.
