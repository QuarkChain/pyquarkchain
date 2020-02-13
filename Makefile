all: grpc qkchash


grpc: quarkchain/proto/grpc.proto
	python -m grpc_tools.protoc --proto_path=quarkchain/proto --python_out=quarkchain/generated --grpc_python_out=quarkchain/generated grpc.proto

.PHONY: qkchash
qkchash:
	$(MAKE) -C qkchash

clean:
	rm -f quarkchain/generated/grpc_pb2.py
	rm -f quarkchain/generated/grpc_pb2_grpc.py
	$(MAKE) -C qkchash clean
