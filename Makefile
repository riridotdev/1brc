.PHONY: build
build:
	@echo 'Building 1brc'
	@go build -o=./bin/1brc .

.PHONY: profile
profile:
	go tool pprof --http localhost:8080 ./bin/1brc cpu.profile
