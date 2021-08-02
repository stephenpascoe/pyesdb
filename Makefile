
PROTOS_DIR=./protos
PACKAGE_DIR=./pyesdb/pb

PROTOS=streams shared status code cluster gossip monitoring operations persistent projections users
PROTOS_PY=$(foreach var,$(PROTOS),$(PACKAGE_DIR)/$(var)_pb2_grpc.py )

.PHONEY: fiximports

all:
	make protos
	make fiximports


protos: $(PROTOS_PY)


${PACKAGE_DIR}/%_pb2_grpc.py:
	python -m grpc_tools.protoc -I${PROTOS_DIR}  --python_out=${PACKAGE_DIR} \
		--grpc_python_out=${PACKAGE_DIR} ${PROTOS_DIR}/$*.proto

fiximports:
	for f in $(PACKAGE_DIR)/*_pb2*.py ; \
	  do sed  's/^import \(.*_pb2\) as \(.*\)/from . import \1 as \2/' $$f >$$f.tmp ; \
		mv $$f.tmp $$f ; \
	done
