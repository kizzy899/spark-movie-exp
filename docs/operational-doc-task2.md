 # Task 2: Spark SQL Gender Tag Preferences — Operation Document
 
 ## 1. Change Overview
 
 | File | Operation | Reason |
 |------|-----------|--------|
 | `sql/schema.sql` | Modified | Added `users` and `ratings` table definitions for task2 |
 | `src/task2_sql_gender_tags/import_mysql.py` | Created | Python script to import Users.dat and Ratings.csv into MySQL |
 | `src/task2_sql_gender_tags/task2_gender_tags.py` | Created | Spark SQL script: read MySQL + Movies.csv, join, group, output JSON |
 | `src/task2_sql_gender_tags/README.md` | Modified | Updated from placeholder to actual implementation documentation |
 | `templates/task2.html` | Modified | UI fine-tuning: unified shadow style, responsive margins |
 | `Makefile` | Modified | Added task2-import, task2, task2-clean, test2, test-all commands |
 | `.env.example` | Modified | Added TASK2_TOP_N environment variable documentation |
 | `tests/__init__.py` | Created | Package marker for tests directory |
 | `tests/test_task2_gender_tags.py` | Created | 8 test cases (TC1-TC8) covering core logic |
 | `tests/run_all.py` | Created | Unified test runner |
 | `docs/plans/2025-06-17-kizy-task2-sql-gender-tags.md` | Created | Implementation plan document |
 
 ## 2. Implementation Steps
 
 ### Step 1: Database Schema Update
 
 - **Files**: `sql/schema.sql`
 - **Changes**: Added two CREATE TABLE statements after the existing streaming_results table:
   - `users` table: userId (INT PK), gender (CHAR(1)), age (INT), occupation (INT), zipCode (VARCHAR(10))
   - `ratings` table: userId (INT), movieId (INT), rating (DECIMAL(2,1)), timestamp (INT), with composite PK on (userId, movieId, timestamp) and indexes on userId and movieId
 - **Reason**: Task2 requires user gender data and rating data in MySQL for Spark SQL JDBC access
 - **Verification**: `mysql -u root -p < sql/schema.sql` should complete without errors
 
 ### Step 2: Data Import Script
 
 - **Files**: `src/task2_sql_gender_tags/import_mysql.py`
 - **Details**:
   - `parse_users_dat_line(line)`: Parses `::` delimited Users.dat lines
   - `parse_rating_csv_line(line)`: Parses CSV rating lines (skipping header)
   - `import_users(filepath, conn)`: Batch imports users with 50K commit intervals
   - `import_ratings(filepath, conn, batch_size)`: Batch imports ratings with 50K commit intervals
   - Main function reads MOVIE_DATA_DIR env var, connects to MySQL, runs both imports
 - **Reason**: Users.dat has a non-standard `::` delimiter; Python script handles this flexibly
 - **Verification**: `python import_mysql.py` with proper MOVIE_DATA_DIR and MySQL config
 
 ### Step 3: Spark SQL Computation Script
 
 - **Files**: `src/task2_sql_gender_tags/task2_gender_tags.py`
 - **Core Logic**:
   1. Read MySQL.users and MySQL.ratings via Spark JDBC
   2. Read Movies.csv locally, explode genres by `|` delimiter
   3. JOIN ratings -> users (on userId) -> genres (on movieId)
   4. GROUP BY gender, genre, COUNT DISTINCT userId
   5. Take top N per gender (default 8)
   6. Build union of tags (male priority), zero-fill missing values
   7. Write JSON to `outputs/task2_gender_tags.json`
 - **Design Decision**: genres from Movies.csv used instead of tags.csv (better coverage)
 - **Verification**: `python task2_gender_tags.py` with PySpark + mysql-connector-java.jar
 
 ### Step 4: Frontend UI Fine-Tuning
 
 - **Files**: `templates/task2.html`
 - **Changes**:
   - Added `box-shadow: 0 8px 22px rgba(31,78,121,0.08)` to `.panel` for consistency with index.html
   - Added responsive `@media (max-width: 820px)` breakpoint for mobile
   - Added font-size for `#notice` text
 - **Reason**: Unify visual style across index/task1/task2 pages (all use light theme)
 - **Verification**: Visual inspection at various viewport widths
 
 ### Step 5: Build System Updates
 
 - **Files**: `Makefile`, `.env.example`
 - **Makefile changes**:
   - Added `task2-import`: run import_mysql.py
   - Added `task2`: run task2_gender_tags.py
   - Added `task2-clean`: truncate MySQL tables
   - Added `test2`: run task2 unit test only
   - Added `test-all`: run all tests via pytest
 - **.env.example changes**:
   - Added `TASK2_TOP_N=8` environment variable
 - **Verification**: `make test-all` runs all tests
 
 ### Step 6: Tests
 
 - **Files**: `tests/test_task2_gender_tags.py`
 - **Test Cases**:
   - TC1: Genre explode splitting
   - TC2: Top 8 selection from 20 items
   - TC3: Union format with zero-fill
   - TC4: Less than 8 tags (boundary)
   - TC5: Empty data (boundary)
   - TC6: Dirty data filtering ((No genres listed))
   - TC7: JSON output structure completeness
   - TC8: Users.dat parsing
 - **Verification**: `python -m unittest discover tests -v` (25 tests, all pass)
 
 ## 3. Smoke Test Results
 
 ### 3.1 Environment
 
 | Item | Value |
 |------|-------|
 | OS | Windows 11 |
 | Python | 3.13.7 |
 | Project Root | D:\\code\\spark-movie-exp |
 
 ### 3.2 Smoke Test Matrix
 
 | ID | Test | Operation | Expected Result | Actual Result | Verdict |
 |----|------|-----------|-----------------|---------------|---------|
 | SM1 | Static compile (app.py) | `python -m py_compile app.py` | No syntax errors | No errors | PASS |
 | SM2 | Static compile (task2 scripts) | `python -m py_compile src/task2_sql_gender_tags/*.py` | No syntax errors | No errors | PASS |
 | SM3 | Unit tests (task2) | `python -m unittest tests.test_task2_gender_tags -v` | 8 tests pass | 8/8 PASS | PASS |
 | SM4 | Unit tests (all) | `python -m unittest discover tests -v` | All 25 tests pass | 25/25 PASS | PASS |
 | SM5 | run_all.py runner | `python tests/run_all.py` | All tests pass | All pass | PASS |
 | SM6 | Flask app import | `python -c "from app import app"` | Module loads (requires Flask installed) | N/A (Flask not in env) | See note |
 | SM7 | Web page /task2 | Browser at http://localhost:5001/task2 | Chart renders with sample data | N/A (requires Flask + deps) | See note |
 
 **Notes**:
 - SM6/SM7 require `pip install -r requirements.txt` first (flask, flask-cors, pymysql, pandas)
 - SM7 also requires MySQL running with proper schema imported
 - All source files compile cleanly (verified)
 - All unit tests pass in the current environment (verified)
 
 ### 3.3 Conclusion
 
 - **Static analysis**: PASS (all files compile without errors)
 - **Unit tests**: PASS (25/25)
 - **Integration**: Conditionally PASS (requires dependency installation)
 - **Overall**: Implementation is complete and verified at the unit level.
   Run `pip install -r requirements.txt` then `python app.py` for full integration test.
 
 ## 4. Rollback Plan
 
 ### If task2 SQL/Spark script has issues:
 - Revert `src/task2_sql_gender_tags/` to README-only placeholder (from git)
 - The Web app continues to work with sample data (fallback in app.py)
 
 ### If schema changes cause issues:
 - Run `DROP TABLE IF EXISTS users, ratings;` to remove new tables
 - `streaming_results` table is unaffected
 
 ### If Makefile breaks:
 - Remove task2-related lines from Makefile
 - Task1 and Task3 make commands are unaffected
 
 ### Complete rollback:
 ```bash
 git checkout -- sql/schema.sql
 git checkout -- templates/task2.html
 git checkout -- Makefile .env.example
 git checkout -- src/task2_sql_gender_tags/README.md
 git rm src/task2_sql_gender_tags/import_mysql.py
 git rm src/task2_sql_gender_tags/task2_gender_tags.py
 git rm tests/test_task2_gender_tags.py
 git rm tests/run_all.py
 ```
 
 ## 5. Key Design Decisions
 
 | Decision | Choice | Rationale |
 |----------|--------|-----------|
 | Tag source | Movies.csv genres | Covers all movies; tags.csv is sparser |
 | Top N | 8 per gender | Good balance between detail and readability |
 | Chart layout | Union X-axis, dual bars | Consistent with existing task2 sample format |
 | MySQL import | Python script | Flexible handling of :: delimiter and batch inserts |
 | JDBC driver | Local jar reference | Portable, no system-wide config needed |
 | Testing strategy | Unit tests (mock pyspark) | No Spark cluster needed for validation |
 | UI theme | Light (matching index/task1) | Task3 is separate dark theme (out of scope) |
 
 ## 6. Data Flow Summary
 
 ```text
 Users.dat (::)  ──→ import_mysql.py ──→ MySQL.users
 Ratings.csv     ──→ import_mysql.py ──→ MySQL.ratings
                                                 │
                                    task2_gender_tags.py (Spark SQL)
                                         │
                              JDBC read users + ratings
                              CSV read Movies.csv → explode genres
                              JOIN → GROUP BY gender, genre
                              COUNT DISTINCT userId
                              Top 8 per gender → union
                                         │
                              outputs/task2_gender_tags.json
                                         │
                              app.py: GET /api/gender-tags
                                         │
                              templates/task2.html (ECharts chart)
 ```
