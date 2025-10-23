# proglog

install protobuf:
sudo apt update
sudo apt install protobuf-compiler
go get google.golang.org/protobuf/cmd/protoc-gen-go@latest


install grpc and protobuf plugin:
go get -u google.golang.org/grpc
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

compile .proto:
make compile
