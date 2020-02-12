all: generate

generate:
	python -m grpc_tools.protoc --proto_path=./quarkchain/cluster --python_out=./quarkchain/generated --grpc_python_out=./quarkchain/generated grpc_client.proto
	$(MAKE) -C qkchash

clean:
	rm -f ./quarkchain/generated/grpc_client_pb2.py ./quarkchain/generated/grpc_client_pb2_grpc.py
	$(MAKE) -C qkchash clean
