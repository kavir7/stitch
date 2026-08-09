BINARY := bin/queued

.PHONY: build run test race vet clean

build:
	go build -o $(BINARY) ./cmd/queued

run: build
	$(BINARY) --addr :8080 --data data/wal --unsafe-demo

test:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin data .crashtest

.PHONY: crash-test

crash-test:
	bash scripts/crash_test.sh

.PHONY: bench

bench:
	bash scripts/bench.sh
