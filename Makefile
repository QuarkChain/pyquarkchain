all: grpc qkchash


grpc: quarkchain/proto/cluster.proto
	python -m grpc_tools.protoc --proto_path=quarkchain/proto --python_out=quarkchain/generated --grpc_python_out=quarkchain/generated cluster.proto

.PHONY: qkchash
qkchash:
	$(MAKE) -C qkchash

clean:
	rm -f quarkchain/generated/*_pb2.py
	rm -f quarkchain/generated/*_pb2_grpc.py
	$(MAKE) -C qkchash clean
