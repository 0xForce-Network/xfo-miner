APP_NAME := xfo-miner
CMD_PATH := ./cmd/xfo-miner
OUT_DIR := ./bin
BUILD_BIN := $(OUT_DIR)/$(APP_NAME)
XMRIG_DIR ?= ./bin/xmrig
XMRIG_LINUX := $(XMRIG_DIR)/xmrig-linux-amd64
XMRIG_WINDOWS := $(XMRIG_DIR)/xmrig-windows-amd64.exe
XMRIG_DARWIN := $(XMRIG_DIR)/xmrig-darwin-arm64

.PHONY: build build-all package checksums release clean run test vet

build:
	mkdir -p $(OUT_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(BUILD_BIN) $(CMD_PATH)

build-all:
	mkdir -p $(OUT_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(OUT_DIR)/$(APP_NAME)-linux-amd64 $(CMD_PATH)
	GOOS=windows GOARCH=amd64 go build -o $(OUT_DIR)/$(APP_NAME)-windows-amd64.exe $(CMD_PATH)
	GOOS=darwin GOARCH=arm64 go build -o $(OUT_DIR)/$(APP_NAME)-darwin-arm64 $(CMD_PATH)

package: build-all
	rm -rf $(OUT_DIR)/release-linux-amd64 $(OUT_DIR)/release-windows-amd64 $(OUT_DIR)/release-darwin-arm64
	mkdir -p $(OUT_DIR)/release-linux-amd64 $(OUT_DIR)/release-windows-amd64 $(OUT_DIR)/release-darwin-arm64
	cp $(OUT_DIR)/$(APP_NAME)-linux-amd64 $(OUT_DIR)/release-linux-amd64/$(APP_NAME)
	cp $(OUT_DIR)/$(APP_NAME)-windows-amd64.exe $(OUT_DIR)/release-windows-amd64/$(APP_NAME).exe
	cp $(OUT_DIR)/$(APP_NAME)-darwin-arm64 $(OUT_DIR)/release-darwin-arm64/$(APP_NAME)
	cp config.example.json README.md $(OUT_DIR)/release-linux-amd64/
	cp config.example.json README.md $(OUT_DIR)/release-windows-amd64/
	cp config.example.json README.md $(OUT_DIR)/release-darwin-arm64/
	@if [ -f "$(XMRIG_LINUX)" ]; then cp "$(XMRIG_LINUX)" "$(OUT_DIR)/release-linux-amd64/xmrig"; else echo "WARN: missing $(XMRIG_LINUX), packaging without xmrig"; fi
	@if [ -f "$(XMRIG_WINDOWS)" ]; then cp "$(XMRIG_WINDOWS)" "$(OUT_DIR)/release-windows-amd64/xmrig.exe"; else echo "WARN: missing $(XMRIG_WINDOWS), packaging without xmrig"; fi
	@if [ -f "$(XMRIG_DARWIN)" ]; then cp "$(XMRIG_DARWIN)" "$(OUT_DIR)/release-darwin-arm64/xmrig"; else echo "WARN: missing $(XMRIG_DARWIN), packaging without xmrig"; fi
	tar -czf $(OUT_DIR)/$(APP_NAME)-linux-amd64.tar.gz -C $(OUT_DIR)/release-linux-amd64 .
	@if command -v zip >/dev/null 2>&1; then \
		(cd $(OUT_DIR)/release-windows-amd64 && zip -q -r ../$(APP_NAME)-windows-amd64.zip .); \
	else \
		python3 -m zipfile -c $(OUT_DIR)/$(APP_NAME)-windows-amd64.zip \
			$(OUT_DIR)/release-windows-amd64/$(APP_NAME).exe \
			$(OUT_DIR)/release-windows-amd64/config.example.json \
			$(OUT_DIR)/release-windows-amd64/README.md \
			$$( [ -f "$(OUT_DIR)/release-windows-amd64/xmrig.exe" ] && echo "$(OUT_DIR)/release-windows-amd64/xmrig.exe" ); \
	fi
	tar -czf $(OUT_DIR)/$(APP_NAME)-darwin-arm64.tar.gz -C $(OUT_DIR)/release-darwin-arm64 .
	rm -rf $(OUT_DIR)/release-linux-amd64 $(OUT_DIR)/release-windows-amd64 $(OUT_DIR)/release-darwin-arm64

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
