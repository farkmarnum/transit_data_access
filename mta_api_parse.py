#!/usr/bin/python3
import gtfs_parser as gtfs
import sys, os
import inspect

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''
' '    This file gives examples of what can be done with the methods in gtfs_parser
'''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''


mta =  gtfs.MTA_Subway('MTA_Subway')
mta.build()
mta.display()


route_id = '5'
feed = gtfs.Feed(route_id, mta)
feed.print_feed()
