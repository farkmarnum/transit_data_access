# Transit Data Access

See it in action: [Transit Data Access](http://www.markfarnum.com)

Transit Data Access is a powerful web app that gives users access to
the full GTFS realtime data feed of a transit system. Instead of being
forced to wait for a server to respond to queries about arrivals,
route-planning, etc, users already have all the data they need -- as up
to date as the last time they opened the app and had a connection. This
is especially important for subway data, since users often to not have
a reliable mobile data connection when below ground.

To keep the data footprint small for users, an initial "full" data packet is
sent with a full representation of the realtime data (<100KB), and then
every 15 seconds, "update" data packets (<10KB) are sent with just the information
that has changed since the last time the user received data.

This app is containerized with Docker, consisting of four containers:

* **parser**

	Python process fetching and parsing static and realtime GTFS data from MTA API

* **redis**

	shared data store and messaging system for GTFS parsed data, user data, etc

* **web_server**

	NodeJS process serving data to the frontend via Websockets

* **web_client**

	NodeJS/React frontend


How it works
------

#### parser

main.py sets a RedisHandler to push data to web_server, and then loops a schedule,
running static.py's StaticHandler.update() once a day, and realtime.py's RealtimeManager.update()
once every 15 seconds.

realtime.py uses pandas to refactor the data into a more efficient format. It then serializes this "full"
data object with a protocol buffers schema (protobuf/transit_data_access.proto) and compresses it with zlib.
It also creates "update" objects which contain the information needed to get a client up to date
if they've received a recent data packet. An "update" is created for each of the last 20 "full" objects.

The docker image uses a multi-stage build, sourcing from python-slim to keep its size low.


#### redis

We're using the stock redis:5 docker image for this project.
In the future we'll add a custom redis.conf for optimization.


#### web_server

This is a straightforward NodeJS server. It uses 'ioredis' to get the new "full" and "update" data packets from Redis and 'ws' to run a Websocket server to the frontend. This server keeps track of the timestamp of the most recent data that each client has confirmed receiving, and uses this to determine which update to send.

The docker image uses a multi-stage build, sourcing from node-alpine to keep its size low.

#### web_client

This React app interfaces with the backend to get data, and provides a responsive UI giving users the ability to make informed transit decisions. Currently it just shows arrivals, organized by route and destination, but in future it will allow searching by station, route planning, and more.

The docker image uses a multi-stage build, sourcing from node-alpine to keep its size low.

#### Docker

Currently the project is built with docker-compose, and the images then manually pulled onto a server. In the future there will be a CD/CI pipeline with AWS Fargate.

config/example.env gives an example of a config file for the docker environment variables. This file should be named docker.env


Issues/Plans
------

* Frontend dataUpdates are not actually being processed yet (many bugs). Once that's fixed, there needs to be some sort of sort+hashsum check that everything worked.

* The NodeJS code needs better commenting, documentation, and organization

* The backend code needs tests

* Docker needs a robust build/test/deploy pipeline
