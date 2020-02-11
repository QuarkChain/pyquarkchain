
all: grpc

grpc:
	python -m grpc_tools.protoc --proto_path=`pwd`/quarkchain/cluster --python_out=`pwd`/quarkchain/cluster --grpc_python_out=`pwd`/quarkchain/cluster  grpc.proto

	$(MAKE) -C qkchash

clean:
	rm -f grpc
	$(MAKE) -C qkchash clean
