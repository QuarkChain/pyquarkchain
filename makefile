QKCDIR = `pwd`/qkchash
GRPCDIR = `pwd`/quarkchain/cluster
MAKE = make

all: generate subsystem

generate:
	python -m grpc_tools.protoc --proto_path=$(GRPCDIR) --python_out=$(GRPCDIR) --grpc_python_out=$(GRPCDIR) grpc_client.proto

subsystem:
	cd $(QKCDIR) && $(MAKE)

clean:
	rm -f $(QKCDIR)/qkchash libqkchash.so qkchash_llrb
	rm -f $(GRPCDIR)/grpc_client_pb2.py grpc_client_pb2_grpc.py
