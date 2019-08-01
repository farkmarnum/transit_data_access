# Transit Data Access

Transit Data Access is a powerful web app that gives users access to
the full GTFS realtime data feed of a transit system. Instead of being
forced to wait for a server to respond to queries about arrivals,
route-planning, etc, users already have all the data they need -- as up
to date as the last time they opened the app and had a connection. This
is especially important for subway data, since users often to not have
a reliable mobile data connection when below ground.

To keep the data footprint small for users, an initial "full" data packet is
sent with a full representation of the realtime data (<100KB), and then
every 15 seconds, "update" data packets are sent with just the information
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


