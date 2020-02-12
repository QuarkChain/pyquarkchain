all: proto qkchashmake

proto: ./quarkchain/cluster/grpc_client.proto
	python -m grpc_tools.protoc --proto_path=./quarkchain/cluster --python_out=./quarkchain/generated --grpc_python_out=./quarkchain/generated grpc_client.proto

qkchashmake:
	$(MAKE) -C qkchash

clean:
	rm -f ./quarkchain/generated/*.py
	$(MAKE) -C qkchash clean
