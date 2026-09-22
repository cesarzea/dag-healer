.PHONY: install test demo api reset clean

install:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pip install -r requirements-airflow.txt \
	  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.0/constraints-3.11.txt
	.venv/bin/pip install "typing-extensions>=4.15.0"

test:
	PYTHONPATH=src:. AIRFLOW_HOME=$(PWD)/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False .venv/bin/python -m pytest -q

api:
	.venv/bin/python -m uvicorn fake_shop_api.main:app --port 8099

demo:
	./scripts/demo.sh

reset:
	./scripts/reset.sh

clean: reset
	rm -rf .pytest_cache .airflow logs **/__pycache__
