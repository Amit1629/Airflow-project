# Airflow Data Pipeline Project

A local Apache Airflow environment demonstrating a real-world style ETL pipeline: fetching e-commerce funnel data (login → product view → checkout) from a mock FastAPI service, merging it, applying business logic, and conditionally alerting based on the results.

## Overview

This project simulates a daily batch pipeline commonly seen in e-commerce/analytics environments:

1. A **FastAPI mock service** generates realistic funnel data (logins, product page views, checkouts).
2. An **Airflow DAG** fetches this data daily, merges it, evaluates checkout totals, and branches into either a success notification or an alert path.
3. A **simple intro DAG** is also included to demonstrate basic task dependency chaining.

## Architecture

```
┌─────────────────┐        ┌──────────────────────────────┐
│  FastAPI Service │◄───────│   Airflow DAG (daily, cron)  │
│  (main.py)       │  POST  │   api_implementation_dag     │
│                  │        │                              │
│  /loginUsers     │        │  fetch_login  ─┐              │
│  /productUsers   │        │  fetch_product ─┼─► branch ──►│ merge_data ──► notify_team
│  /checkoutUsers  │        │  fetch_checkout─┘     │        │
└─────────────────┘        │                       └──────► alarming_situation
                            └──────────────────────────────┘
```

## Tech Stack

- **Apache Airflow** — orchestration (TaskFlow API, `@dag`/`@task` decorators, branching)
- **FastAPI** — mock data-generating REST API with HTTP Basic Auth
- **Docker / Docker Compose** — local containerized Airflow + API setup
- **Python** — Airflow DAGs, business logic, API client

## Project Structure

```
airflow/
├── api_code/                  # Standalone FastAPI mock data service
│   ├── main.py                 # Endpoints: /loginUsers, /productUsers, /checkoutUsers, /getAll
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   ├── requirements.txt
│   └── test.py / test.json
│
├── config/
│   └── airflow.cfg              # Airflow configuration (not tracked — see .gitignore)
│
├── dags/
│   ├── myfirst_dag.py           # Intro DAG: bash task → python task
│   ├── api_implemention.py      # Main daily pipeline DAG
│   ├── test_dag.py
│   └── include/
│       ├── api_client.py        # Handles authenticated POST requests to the API
│       ├── business_logic.py    # Merges datasets, calculates checkout totals
│       └── utils.py             # JSON file writing helper
│
└── docker-compose.yaml          # Root Airflow service definition
```

## DAGs

### `myfirst_dag`
A minimal two-task DAG for demonstrating task dependencies:
- `copy_file` (BashOperator) → `task2` (PythonOperator)

### `api_implementation_dag`
Runs daily (`0 0 * * *`) and performs:

1. **Parallel fetch** — `fetch_login`, `fetch_product`, `fetch_checkout` each call the FastAPI service via `call_api()` and write results to JSON (`write_json()`).
2. **Branch decision** — `branch_on_amount` sums the checkout data's `total_value` field. If the total is `<= 0`, the pipeline branches to an alert task; otherwise it proceeds to merge.
3. **Merge** — `merge_data` combines all three datasets into a single JSON file via `merge_files()`.
4. **Notify** — on success, `notify_team` runs; on a zero-checkout day, `alarming_situation` runs instead. Both converge at `end` using `trigger_rule="none_failed"`.

This demonstrates conditional branching, the TaskFlow API, dynamic task outputs (XComs via return values), and modular code organization (`include/` package).

## Mock API Service (`api_code/main.py`)

A FastAPI app that generates realistic, randomized funnel data for testing the pipeline without needing a real production data source:

- `POST /loginUsers` — random login events (device, browser, referrer, session duration, geo)
- `POST /productUsers` — product page views for ~80% of logged-in users
- `POST /checkoutUsers` — checkouts for ~60% of users who added items to cart
- `POST /getAll` — returns all three datasets together

Protected with HTTP Basic Auth.

## Running Locally

**Prerequisites:** Docker Desktop, VS Code

1. Clone the repo:
   ```bash
   git clone https://github.com/Amit1629/Airflow-project.git
   cd Airflow-project/airflow
   ```
2. Start the Airflow stack:
   ```bash
   docker-compose up
   ```
3. In a separate terminal, start the mock API service:
   ```bash
   cd api_code
   docker-compose up
   ```
4. Open the Airflow UI at `http://localhost:8080`
5. In **Admin → Variables**, set:
   - `BASE_API_URL` — URL of the running mock API service
   - `API_AUTH_HEADER` — Basic Auth header value for the API
6. Trigger `api_implementation_dag` or `myfirst_dag` from the UI.

## Notes

- Sensitive files (`.env`, `airflow.cfg`, logs, `__pycache__`) are excluded via `.gitignore` and should be configured locally.
- This project is intended as a learning/portfolio pipeline demonstrating Airflow fundamentals: DAG authoring, task dependencies, branching logic, XComs, and integrating an external API within a containerized setup.
