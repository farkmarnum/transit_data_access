all: python js

python:
	bash -c "protoc --python_out=../parser transit_data_access.proto"

js:
	# bash -c "protoc --js_out=import_style=commonjs,binary:../web_server/client/src/ transit_data_access.proto"
	cp transit_data_access.proto ../web_server/client/public/transit_data_access.proto
