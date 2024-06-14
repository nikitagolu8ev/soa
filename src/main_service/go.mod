module main_service

go 1.21.1

replace proto => ../proto

require (
	github.com/go-redis/redis/v8 v8.11.5
	google.golang.org/protobuf v1.34.2
)

require (
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/gorilla/mux v1.8.1
	golang.org/x/crypto v0.24.0
	google.golang.org/grpc v1.64.0
	proto v0.0.0-00010101000000-000000000000
)
