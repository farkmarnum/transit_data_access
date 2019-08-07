all: python js

python:
	bash -c "protoc --python_out=../parser transit_data_access.proto"

js:
	cp transit_data_access.proto ../web_server/client/public/transit_data_access.proto
