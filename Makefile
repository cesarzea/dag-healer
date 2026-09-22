PY_VERSION := $(shell python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
AIRFLOW_VERSION := $(shell sed -n 's/^apache-airflow==//p' requirements-airflow.txt)
CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PY_VERSION).txt

.PHONY: install test demo api reset clean versions

versions:
	@echo "python     $(PY_VERSION)"
	@echo "airflow    $(AIRFLOW_VERSION)"
	@echo "constraints $(CONSTRAINTS)"

install:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pip install -r requirements-airflow.txt --constraint $(CONSTRAINTS)
	@echo
	@echo "installed airflow $(AIRFLOW_VERSION) on python $(PY_VERSION)"

test:
	PYTHONPATH=src:. AIRFLOW_HOME=$(PWD)/.airflow AIRFLOW__CORE__LOAD_EXAMPLES=False \
	  .venv/bin/python -m pytest -q

api:
	.venv/bin/python -m uvicorn fake_shop_api.main:app --port 8099

demo:
	./scripts/demo.sh

reset:
	./scripts/reset.sh

clean: reset
	rm -rf .pytest_cache .airflow logs
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
