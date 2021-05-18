hello_world:
	pytest test/exercise/test_canary.py
.PHONY: hello_world

data/base:
	spark-submit src/playground/generation.py

test:
	pytest test/exercise/
.PHONY: test