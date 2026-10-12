PYTHON ?= python3.11

.PHONY: test lint coverage mutation quality live-supervisor

test:
	PYTHONPATH=src "$(PYTHON)" -m unittest discover -s tests -v

lint:
	ruff format --check .
	ruff check .

coverage:
	coverage erase
	PYTHONPATH=src coverage run -m unittest discover -s tests
	coverage report

mutation:
	mutmut run
	mutmut export-cicd-stats
	"$(PYTHON)" scripts/check_mutation_score.py \
		--stats mutants/mutmut-cicd-stats.json --minimum 80

quality: lint coverage

live-supervisor:
	PYTHONPATH=src "$(PYTHON)" scripts/check_live_supervisor.py
