PYTHON ?= .venv/bin/python
UV ?= uv
MUTMUT ?= "$(PYTHON)" scripts/run_mutmut.py
MUTMUT_RUN_ARGS ?= --max-children 4

.PHONY: test lint coverage mutation mutation-clean quality dependency-lock dependency-audit candidate-test gate-test integration-test service-sidecar-test live-supervisor
.PHONY: require-git

require-git:
	@command -v git >/dev/null 2>&1 || { echo "Git is required for this gate" >&2; exit 2; }

test: require-git
	PYTHONPATH=src "$(PYTHON)" -m unittest discover -s tests -v

lint:
	"$(PYTHON)" -m ruff format --check .
	"$(PYTHON)" -m ruff check .

coverage: require-git
	"$(PYTHON)" -m coverage erase
	PYTHONPATH=src "$(PYTHON)" -m coverage run -m unittest discover -s tests
	"$(PYTHON)" -m coverage report

mutation: require-git
	PYTHONDONTWRITEBYTECODE=1 $(MUTMUT) run $(MUTMUT_RUN_ARGS)
	$(MUTMUT) export-cicd-stats
	"$(PYTHON)" scripts/check_mutation_score.py \
		--stats mutants/mutmut-cicd-stats.json --minimum 80

mutation-clean:
	@if [ -d mutants ]; then find mutants -depth -delete; fi
	@find src tests -depth \( -type d -name __pycache__ -o -path '*/__pycache__/*' \) -delete
	$(MAKE) PYTHON="$(PYTHON)" mutation

quality: lint coverage

dependency-lock:
	$(UV) lock --python 3.11
	$(UV) export --frozen --extra development --no-emit-project \
		--format requirements-txt --output-file requirements-development.lock

dependency-audit:
	"$(PYTHON)" -m pip_audit -r requirements-development.lock --disable-pip

candidate-test: require-git
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_candidates -v

gate-test: require-git
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_integration_gates -v

integration-test: require-git
	PYTHONPATH=src "$(PYTHON)" -m unittest tests.test_integration_queue -v

service-sidecar-test:
	PYTHONPATH=src "$(PYTHON)" -m unittest \
		tests.test_service_sidecars \
		tests.test_service_sidecar_storage \
		tests.test_service_sidecar_service \
		tests.test_live_service_sidecar -v

live-supervisor:
	PYTHONPATH=src "$(PYTHON)" scripts/check_live_supervisor.py
