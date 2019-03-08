# gtfs_parser

This project is in its early stages. The end goal is to create an app that helps transit goers find their best route using analysis of real-time GTFS data.

Structure so far:
gtfs_parser.py
1. introduces these classes:

   Stop, Shape, Route, and TransitSystem to describe the building blocks of a transit system

   ShapeBuilder, RouteBuilder, and TransitSystemBuilder, superclasses holding functions for initializing their subclasses based on static GTFS data

   Feed, for accessing GTFS realtime feeds

   Train, to describe position and other attributes of a train in realtime

   MTA_Subway and MBTA_Subway, subclasses of TransitSystem with the specific configuration and connection info necessary to get GTFS data for those systems

2. and introduces these methods:

   TransitSystem.update() loads a new static GTFS feed and consolidates some of the data into additional CSVs

   TransitSystem.build() parses the GTFS data in the CSVs to populate TransitSystem with Route, Shape, Branch, and Stop objects for the full system

   TransitSystem.display() produces a text representation of the full system by recursively calling the other objects' display() method

   TransitSystem.map_each_route() produces a visual representation (currently PDF) of the map of connections within each route, using dot


mta_example.py
 * Example of a Python script that uses gtfs_parser classes and methods


requirements.txt
 * list of required python packages
 * generated with: pip -l freeze > requirements.txt
