grpc: setup
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/rm.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/nm.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/simulator.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/frontend.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/backend.proto
# I get confused between grpc and rcp
rpc: setup
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/rm.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/nm.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/simulator.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/frontend.proto
	python -m grpc_tools.protoc -I./grpc_proto --python_out=./grpc_stubs --grpc_python_out=./grpc_stubs ./grpc_proto/backend.proto
clean:
	rm ./grpc_stubs/*_pb2.py
	rm ./grpc_stubs/*_pb2_grpc.py
setup:
	mkdir -p grpc_stubs
