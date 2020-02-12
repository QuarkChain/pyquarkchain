all: grpc

grpc:
	python -m grpc_tools.protoc --proto_path=quarkchain/proto --python_out=quarkchain/generated --grpc_python_out=quarkchain/generated grpc.proto
	$(MAKE) -C qkchash

clean:
	rm -f quarkchain/generated/grpc_pb2.py
	rm -f quarkchain/generated/grpc_pb2_grpc.py
	$(MAKE) -C qkchash clean
