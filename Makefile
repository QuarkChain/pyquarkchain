all: grpc

grpc:
	python -m grpc_tools.protoc --proto_path=`pwd`/quarkchain/proto --python_out=`pwd`/quarkchain/generated --grpc_python_out=`pwd`/quarkchain/generated  grpc.proto
	$(MAKE) -C qkchash

clean:
	rm -f `pwd`/quarkchain/generated/grpc_pb2.py
	rm -f `pwd`/quarkchain/generated/grpc_pb2_grpc.py
	$(MAKE) -C qkchash clean
