all: grpc

grpc:
	python -m grpc_tools.protoc --proto_path=`pwd`/quarkchain/cluster --python_out=`pwd`/quarkchain/cluster/tests --grpc_python_out=`pwd`/quarkchain/cluster/tests  grpc.proto
	$(MAKE) -C qkchash

clean:
	rm -f `pwd`/quarkchain/cluster/tests/grpc_pb2.py
	rm -f `pwd`/quarkchain/cluster/tests/grpc_pb2_grpc.py
	$(MAKE) -C qkchash clean
