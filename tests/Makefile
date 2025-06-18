RUN_TIMES := 10

.PHONY: e2e fuzzing all

e2e:
	pytest e2e/

fuzzing:
	for i in $(shell seq 1 ${RUN_TIMES}); \
	do \
		echo "Running test iteration $$i"; \
		pytest fuzzing/; \
	done

all: fuzzing e2e
