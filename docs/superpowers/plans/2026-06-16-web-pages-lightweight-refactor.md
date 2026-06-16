# Web Pages Lightweight Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lightweight Web structure with one overview page and three task pages for the Spark movie experiment.

**Architecture:** Keep the existing Flask-based approach. Add a unified root `app.py` with routes for overview, task 1, task 2, and task 3 while preserving the existing task-specific files so earlier work remains usable.

**Tech Stack:** Python, Flask, PyMySQL, HTML, CSS, JavaScript, ECharts.

---

### Task 1: Unified Flask Entry

**Files:**
- Modify: `app.py`
- Read: `src/task1_rdd_top20/top20_output.json`
- Read: `index.html`

- [ ] Add routes `/`, `/task1`, `/task2`, `/task3`.
- [ ] Keep `/api/latest` for task 3 streaming data.
- [ ] Add `/api/top20` for task 1 result data.
- [ ] Add `/api/gender-tags` as a mock-friendly placeholder until task 2 Spark SQL output exists.
- [ ] Verify routes can be imported without starting Flask.

### Task 2: Web Pages

**Files:**
- Create: `templates/index.html`
- Create: `templates/task1.html`
- Create: `templates/task2.html`
- Create: `templates/task3.html`

- [ ] Build an overview page with links to three tasks.
- [ ] Build task 1 table page that calls `/api/top20`.
- [ ] Build task 2 ECharts bar chart page that calls `/api/gender-tags`.
- [ ] Move the existing task 3 ECharts page into the unified template route.

### Task 3: Project Guidance

**Files:**
- Create: `requirements.txt`
- Modify: `README.md`

- [ ] Add Python dependencies for the current Flask/Spark scripts.
- [ ] Update README with current status, page routes, and quick start commands.
- [ ] Clearly mark task 2 as pending Spark SQL integration.

### Task 4: Verification

**Files:**
- Read: `app.py`

- [ ] Run `python -m py_compile app.py streaming_job.py feed_data.py split_data.py`.
- [ ] Import Flask app and list registered routes.
- [ ] Report any verification that cannot run because dependencies or external services are missing.
