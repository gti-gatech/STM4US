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
    
    def __init__(self, PARAMS, printer, querywriter):
    
      self.main_elements = PARAMS["MAIN_ELEMENTS"]
      self.osm_fields = PARAMS["OSM_FIELDS"]
      self.datasetid = PARAMS["DATASETID"]
      self.printer = printer
      self.querywriter = querywriter
      self.currElement = {}
      self.tags = {self.osm_fields["node"]: set(), 
                   self.osm_fields["way"]: set(), 
                   self.osm_fields["relation"]: set()}
      
    
    # Call when an element starts
    def startElement(self, name, attributes):

        # self.CurrentData = name
        # self.printer.send(("start",(name, attributes._attrs)))
        if name == self.osm_fields["node"]:

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
            self.currElement[self.osm_fields["type"]] = name
            self.currElement[self.osm_fields["id"]] = attributes[self.osm_fields["id"]]
            self.currElement[self.osm_fields["tags"]] = {self.osm_fields["lat"]: attributes[self.osm_fields["lat"]], 
                                                         self.osm_fields["lon"]: attributes[self.osm_fields["lon"]]}

        elif name == self.osm_fields["way"]:

            if self.currElement:
                raise Exception("New element started before last element ended.")
            
            """
            print("************************* Way **************************")
            wayID = attributes["id"]
            print("Way ID:", wayID)
            """

            self.currElement[self.osm_fields["type"]] = name
            self.currElement[self.osm_fields["id"]] = attributes[self.osm_fields["id"]]
            self.currElement[self.osm_fields["tags"]] = {}
            self.currElement[self.osm_fields["nodes"]] = []

        elif name == self.osm_fields["relation"]:

            if self.currElement:
                raise Exception("New element started before last element ended.")
            
            """
            print("*********************** Relation ***********************")
            relationID = attributes["id"]
            print("Relation ID:", relationID)
            """

            self.currElement[self.osm_fields["type"]] = name
            self.currElement[self.osm_fields["id"]] = attributes[self.osm_fields["id"]]
            self.currElement[self.osm_fields["tags"]] = {}
            self.currElement[self.osm_fields["members"]] = []

        elif name == self.osm_fields["nd"]:

            if not self.currElement:
                raise Exception("Property of element started without element.")
            
            """
            ndRef = attributes["ref"]
            print("  nd ref:", ndRef)
            """

            self.currElement[self.osm_fields["nodes"]].append(attributes[self.osm_fields["ref"]])

        elif name == self.osm_fields["member"]:

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
                self.osm_fields["type"]: attributes[self.osm_fields["type"]],
                self.osm_fields["ref"]: attributes[self.osm_fields["ref"]],
                self.osm_fields["role"]: attributes[self.osm_fields["role"]]
            }
            self.currElement[self.osm_fields["members"]].append(dict)

        elif name == self.osm_fields["tag"]:

            if not self.currElement:
                raise Exception("Property of element started without element.")
            
            """
            tagKey = attributes["k"]
            tagValue = attributes["v"]

            printStr = "  key: " + tagKey + " ||" + " value: " + tagValue
            print(printStr)
            """

            #if not self.ignore_tags:
            self.currElement[self.osm_fields["tags"]][attributes[self.osm_fields["k"]]] \
                = attributes[self.osm_fields["v"]]
            
            self.tags[self.currElement[self.osm_fields["type"]]].add(attributes[self.osm_fields["k"]])


    # Call when an elements ends
    def endElement(self, name):
        
        #self.CurrentData = ""
        # self.printer.send(("end", name))
        if name in self.main_elements:

            # send data to a graph database
            self.querywriter.send(self.currElement)

            self.currElement = {}
            
        elif name == self.osm_fields["osm"]:
            self.querywriter.send((self.tags, self.datasetid))

        return

    # Call when a character is read
    def characters(self, text):

        # do something with the characters in the element

        #self.printer.send(("text", text))

        return
