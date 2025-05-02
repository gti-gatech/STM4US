"""
OpenStreetMap (OSM) XML data parser using Python SAX event driven style.

The script parses OSM data from a .osm or .xml file extension, produces nodes,
ways, relations, nd, member and tags as events, and store these events to a 
local Neo4j database: "neo4j://localhost:7687" or an AWS Neptune database:
    "bolt://<database-name.cluster-id>.<region>.neptune.amazonaws.com:8182"

For more information on the Python driver for Neo4j, visit:
    https://pypi.org/project/neo4j-driver/
    https://neo4j.com/docs/api/python-driver/current/
    https://neo4j.com/docs/python-manual/current/

"""

import xml.sax

class OsmDataHandler( xml.sax.ContentHandler ):

    # END_POINT = "(End Point URL)"
    MAIN_ELEMENTS = ["node", "way", "relation"]
    
    def __init__(self, printer, querywriter,dataset_id,ignore_tags):
    
      #self.CurrentData = ""
      self.printer = printer
      self.querywriter = querywriter
      self.currElement = {}
      self.tags = {"node":set(),"way":set(),"relation":set()}
      self.dataset_id = dataset_id
      self.ignore_tags = ignore_tags
    
    # Call when an element starts
    def startElement(self, name, attributes):

        #self.CurrentData = name
        # self.printer.send(('start',(name, attributes._attrs)))
        if name == "node":
            if self.currElement:
                raise Exception("New element started before last element ended.")
            """
            print("************************* Node *************************")
            nodeID = attributes["id"]
            nodeLat = attributes["lat"]
            nodeLon = attributes["lon"]

            printStr = "Node ID: " + nodeID + " lat: " + nodeLat + " lon: " + nodeLon
            print(printStr)
            """
            self.currElement['type'] = name
            self.currElement['id'] = attributes['id']
            self.currElement['tags'] = {'lat':attributes['lat'], 'lon': attributes['lon']}

        elif name == "way":
            if self.currElement:
                raise Exception("New element started before last element ended.")
            """
            print("************************* Way **************************")
            wayID = attributes["id"]
            print("Way ID:", wayID)
            """
            self.currElement['type'] = name
            self.currElement['id'] = attributes['id']
            self.currElement['tags'] = {}
            self.currElement['nodes'] = []

        elif name == "relation":
            if self.currElement:
                raise Exception("New element started before last element ended.")
            """
            print("*********************** Relation ***********************")
            relationID = attributes["id"]
            print("Relation ID:", relationID)
            """
            self.currElement['type'] = name
            self.currElement['id'] = attributes['id']
            self.currElement['tags'] = {}
            self.currElement['members'] = []

        elif name == "nd":
            if not self.currElement:
                raise Exception("Property of element started without element.")
            """
            ndRef = attributes["ref"]
            print("  nd ref:", ndRef)
            """
            self.currElement['nodes'].append(attributes["ref"])

        elif name == "member":
            if not self.currElement:
                raise Exception("Property of element started without element.")
            """
            memberType = attributes["type"]
            memberRef = attributes["ref"]
            memberRole = attributes["role"]

            printStr = "  type: " + memberType + " ||" + " ref: " + memberRef + " ||" + " role: " + memberRole
            print(printStr)
            """
            dict = {
                'type': attributes['type'],
                'ref': attributes['ref'],
                'role': attributes['role']
            }
            self.currElement['members'].append(dict)

        elif name == "tag":
            if not self.currElement:
                raise Exception("Property of element started without element.")
            """
            tagKey = attributes["k"]
            tagValue = attributes["v"]

            printStr = "  key: " + tagKey + " ||" + " value: " + tagValue
            print(printStr)
            """
            if not self.ignore_tags:
                self.currElement['tags'][attributes['k']] = attributes['v']
                self.tags[self.currElement['type']].add(attributes['k'])



    # Call when an elements ends
    def endElement(self, name):
        
        #self.CurrentData = ""
        # self.printer.send(('end', name))
        if name in self.MAIN_ELEMENTS:

            # send data to a graph database
            self.querywriter.send(self.currElement)

            self.currElement = {}
        elif name == 'osm':
            self.querywriter.send((self.tags, self.dataset_id))
        return

   # Call when a character is read
    def characters(self, text):

        # do something with the characters in the element

        #self.printer.send(('text', text))

        return
