module post_service

go 1.21.1

replace proto => ../proto

require proto v0.0.0-00010101000000-000000000000

require (
	github.com/lib/pq v1.10.9
	golang.org/x/net v0.22.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.2 // indirect
)
