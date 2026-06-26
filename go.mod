module github.com/carissaayo/go-kv-dist

go 1.25.0

require (
	github.com/carissaayo/go-durable-kv v0.0.0
	go.etcd.io/raft/v3 v3.6.0
)

require (
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace github.com/carissaayo/go-durable-kv => "../personal/go projects/go-kv-store"

require github.com/gogo/protobuf v1.3.2
