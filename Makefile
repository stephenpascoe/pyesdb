
PROTOS_DIR=./protos
PACKAGE_DIR=./pyesdb/pb

.PHONY: fiximports protos descriptor

all:
	make protos
	make fiximports

descriptor:
	protoc --descriptor_set_out ${PACKAGE_DIR}/descriptor.protoset --proto_path=${PROTOS_DIR} \
	--include_imports ${PROTOS_DIR}/*.proto


protos: descriptor
	python -m grpc_tools.protoc --proto_path=${PROTOS_DIR} --python_out=${PACKAGE_DIR} \
		--grpc_python_out=${PACKAGE_DIR} ${PROTOS_DIR}/*.proto

fiximports:
	for f in $(PACKAGE_DIR)/*_pb2*.py ; \
	  do sed  's/^import \(.*_pb2\) as \(.*\)/from . import \1 as \2/' $$f >$$f.tmp ; \
		mv $$f.tmp $$f ; \
	done

clean:
	rm -rf $(PACKAGE_DIR)/*
