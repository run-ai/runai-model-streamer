RUN_TIMES := 10

.PHONY: fuzzing all

fuzzing:
	for i in $(shell seq 1 ${RUN_TIMES}); \
	do \
		echo "Running test iteration $$i"; \
		python3 -m unittest discover -b fuzzing; \
	done

all: fuzzing
