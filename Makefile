PYTHON ?= .venv/bin/python
MUTMUT_RUN_ARGS ?=

.PHONY: test lint coverage mutation mutation-clean quality candidate-test gate-test integration-test service-sidecar-test live-supervisor

test:
	PYTHONPATH=src "$(PYTHON)" -m unittest discover -s tests -v

lint:
	"$(PYTHON)" -m ruff format --check .
	"$(PYTHON)" -m ruff check .

coverage:
	"$(PYTHON)" -m coverage erase
	PYTHONPATH=src "$(PYTHON)" -m coverage run -m unittest discover -s tests
	"$(PYTHON)" -m coverage report

mutation:
	PYTHONDONTWRITEBYTECODE=1 "$(PYTHON)" -m mutmut run $(MUTMUT_RUN_ARGS)
	"$(PYTHON)" -m mutmut export-cicd-stats
	"$(PYTHON)" scripts/check_mutation_score.py \
		--stats mutants/mutmut-cicd-stats.json --minimum 80

mutation-clean:
	@if [ -d mutants ]; then find mutants -depth -delete; fi
	@find src tests -depth \( -type d -name __pycache__ -o -path '*/__pycache__/*' \) -delete
	$(MAKE) PYTHON="$(PYTHON)" mutation

quality: lint coverage

candidate-test:
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_candidates -v

gate-test:
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_integration_gates -v

integration-test:
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_integration_queue -v

service-sidecar-test:
	PYTHONPATH=src "$(PYTHON)" -m unittest \
		tests.test_service_sidecars \
		tests.test_service_sidecar_storage \
		tests.test_service_sidecar_service \
		tests.test_live_service_sidecar -v

live-supervisor:
	PYTHONPATH=src "$(PYTHON)" scripts/check_live_supervisor.py
