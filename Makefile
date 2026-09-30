.PHONY: run test clean

run:
	python3 -m etl.pipeline

test:
	python3 -m unittest discover -s tests -v

clean:
	rm -rf output/*.db output/*.md __pycache__ etl/__pycache__ tests/__pycache__
