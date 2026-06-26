module github.com/carissaayo/go-kv-dist

go 1.25.0

require (
	github.com/carissaayo/go-durable-kv v0.0.0
	go.etcd.io/raft/v3 v3.6.0
)

require (
	github.com/golang/protobuf v1.5.4 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)

replace github.com/carissaayo/go-durable-kv => "../personal/go projects/go-kv-store"

require (
	github.com/gogo/protobuf v1.3.2
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)
