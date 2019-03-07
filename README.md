# gtfs_parser

This project is in its early stages. The end goal is to create an app that helps transit goers find their best route using analysis of real-time GTFS data.

Structure so far:

gtfs_parser.py
  - introduces the following classes:
     - Stop, Shape, Route, and TransitSystem to describe the building blocks of a transit system
     - ShapeBuilder, RouteBuilder, and TransitSystemBuilder, superclasses holding functions for initializing their subclasses based on static GTFS data
     - Feed, for accessing GTFS realtime feeds
     - Train, to describe position and other attributes of a train in realtime
     - MTA_Subway and MBTA_Subway, subclasses of TransitSystem with the specific configuration and connection info necessary to get GTFS data for those systems

project_functions.py
  - Holds utility functions for gtfs_parser.py

mta_example.py
  - Example of a Python script that uses gtfs_parser classes and methods


requirements.txt
 - list of required python packages
 - generated with: pip -l freeze > requirements.txt
