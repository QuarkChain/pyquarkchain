
all: grpc

grpc: grpc.proto
	python -m grpc_tools.protoc --proto_path=quarkchain/cluster --python_out=quarkchain/cluster --grpc_python_out=quarkchain/cluster  grpc.proto

	$(MAKE) -C qkchash

clean:
	rm -f grpc
	$(MAKE) -C qkchash clean
