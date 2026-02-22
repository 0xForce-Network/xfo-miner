APP_NAME := xfo-miner
CMD_PATH := ./cmd/xfo-miner
OUT_DIR := ./bin

.PHONY: build build-all package checksums release clean run test vet

build:
	mkdir -p $(OUT_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/$(APP_NAME)-linux-amd64 $(CMD_PATH)

build-all:
	mkdir -p $(OUT_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/$(APP_NAME)-linux-amd64 $(CMD_PATH)
	GOOS=windows GOARCH=amd64 go build -o $(OUT_DIR)/$(APP_NAME)-windows-amd64.exe $(CMD_PATH)
	GOOS=darwin GOARCH=arm64 go build -o $(OUT_DIR)/$(APP_NAME)-darwin-arm64 $(CMD_PATH)

package: build-all
	tar -czf $(OUT_DIR)/$(APP_NAME)-linux-amd64.tar.gz -C $(OUT_DIR) $(APP_NAME)-linux-amd64
	zip -j $(OUT_DIR)/$(APP_NAME)-windows-amd64.zip $(OUT_DIR)/$(APP_NAME)-windows-amd64.exe
	tar -czf $(OUT_DIR)/$(APP_NAME)-darwin-arm64.tar.gz -C $(OUT_DIR) $(APP_NAME)-darwin-arm64

checksums: package
	sha256sum \
		$(OUT_DIR)/$(APP_NAME)-linux-amd64.tar.gz \
		$(OUT_DIR)/$(APP_NAME)-windows-amd64.zip \
		$(OUT_DIR)/$(APP_NAME)-darwin-arm64.tar.gz \
		> $(OUT_DIR)/SHA256SUMS

release: clean checksums

run:
	go run $(CMD_PATH) -config ./config.example.json

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf $(OUT_DIR)
