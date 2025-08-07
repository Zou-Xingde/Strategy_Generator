Project Cleanup Report

Date: 2025-08-07

Scope
- Removed legacy scripts related to swing generation/rebuild, data rebuild/migration flows, and tests.
- Preserved core libraries under `src/algorithms`, `src/data_processing`, database layer, frontend, and config.

Frontend remains intact
- App: `src/frontend/app.py`
- Template: `src/frontend/templates/index.html`
- Static: `src/frontend/static/app.js`, `src/frontend/static/style.css`
- Start script: `start_frontend.py`

Removed files

1) Swing generation / rebuild / processing (root)
- process_zigzag_swings.py
- generate_m5_swings.py
- generate_m5_swings_optimized.py
- generate_m5_swings_simple.py
- regenerate_m5_only.py
- regenerate_all_m5_swings.py
- regenerate_m5_swings.py
- regenerate_complete_swings.py
- regenerate_swing_data.py
- rebuild_swing_data_batch.py
- batch_rebuild_all_timeframes.py
- fix_and_rebuild_timeframes.py
- complete_rebuild_all_symbols.py
- final_complete_rebuild.py
- process_all_data.py
- process_d1_data.py
- quick_process_sample.py

2) Data rebuild and migration (root and scripts)
- rebuild_database.py
- rebuild_database_batch.py
- rebuild_database_continue.py
- database_migration.py
- complete_migration.py
- complete_remaining_migration.py
- final_migration.py
- final_successful_migration.py
- simple_migration.py
- scripts/migrate_database.py
- scripts/setup.py
- scripts/cleanup_database.py
- scripts/cleanup_test_data.py
- scripts/generate_test_data.py

3) Tests
- test_fixed_algorithm.py
- test_improved_zigzag.py
- test_m5_deviation.py
- test_migration.py
- test_performance.py
- test_rebuild_one_symbol.py
- test_regenerate_xauusd_d1.py
- test_single_symbol_swing.py
- test_smart_loading.py
- test_swing_display.py
- test_swing_processing.py
- test_version_control.py
- test_zigzag_simple.py
- tests/test_data_processing.py

Files intentionally kept
- Algorithms: `src/algorithms/*`
- Data processing: `src/data_processing/*`
- Database layer: `src/database/*`
- Config: `config/*`
- Frontend: `src/frontend/*`, `start_frontend.py`
- Utility/inspection: `show_complete_database_structure.py`, `show_full_db.py`, `verify_candlestick_data.py`, `optimize_database.py`, checks under `check_*.py`

Notes
- Log and documentation files were retained: `*.log`, `*.md`.
- If any removed script is needed later, recover via version control history.



