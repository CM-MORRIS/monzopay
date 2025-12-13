# Makefile

# Variable for the binary name
APP_NAME=monzopay

# 1. Generate Protobuf files
proto:
	@echo "Generating Protobuf files..."
	protoc \
  --proto_path=proto \
  --go_out=generated --go_opt=paths=source_relative \
  --go-grpc_out=generated --go-grpc_opt=paths=source_relative \
  payment.proto

# 2. Run the server locally (fast feedback)
run:
	@echo "Running $(APP_NAME)..."
	go run main.go

# 3. Run all tests
test:
	@echo "Running tests..."
	go test -v ./...

# 4. Clean up generated files/binaries
clean:
	@echo "Cleaning..."
	rm -f $(APP_NAME)
	go clean

# 5. Build the binary
build:
	@echo "Building $(APP_NAME)..."
	go build -o $(APP_NAME) main.go

# 6. Docker shortcuts
docker-up:
	docker-compose up -d --build

docker-down:
	docker-compose down

# Declare these as "commands", not files
.PHONY: proto run test clean build docker-up docker-down
