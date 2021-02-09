.PHONY: clean test

PROBE=time -f "\nCPU: %P (%Us)\n" strace -c

test:
	shasum -c test.txt

clean:
	rm -rf data/*.reversed

python: clean
	${PROBE} python reverse.py ${METHOD} ${POOL}

sh: clean
	${PROBE} sh reverse.sh