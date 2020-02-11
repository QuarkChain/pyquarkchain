SUBDIR = /code/pyquakchain/qkchash/
MAKE = make

.PHONY: generate subsystem

	generate:
		python -m grpc_tools.protoc --python_out=. --grpc_python_out=. -I. grpc_client.proto

subsystem:
	cd $(SUBDIR) && $(MAKE)
