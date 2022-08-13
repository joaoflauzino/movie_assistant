.PHONY: help clean clean-build lint test

.DEFAULT: help


test:
	pytest -s --verbose --color=yes --cov=application/collect_data --cov-report term-missing
