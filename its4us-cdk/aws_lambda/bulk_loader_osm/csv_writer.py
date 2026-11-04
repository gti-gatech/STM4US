import math

class OsmAWSBulkLoadCSVWriter:

    def __init__(self, PARAMS):

        self.osm_fields = PARAMS["OSM_FIELDS"]
        self.node_outfile = PARAMS["NODE_OUTFILE"]
        self.way_outfile = PARAMS["WAY_OUTFILE"]
        self.relation_outfile = PARAMS["RELATION_OUTFILE"]
        self.waylink_outfile = PARAMS["WAYLINK_OUTFILE"]
        self.relationlink_outfile = PARAMS["RELATIONLINK_OUTFILE"]
        self.osm_node_label = PARAMS["OSM_NODE_LABEL"]
        self.osm_way_label = PARAMS["OSM_WAY_LABEL"]
        self.osm_relation_label = PARAMS["OSM_RELATION_LABEL"]

        self.osmNodes = []
        self.osmWays = []
        self.osmRelations = []


    def receive(self, data):

        if self.osm_fields["type"] in data:
            if data[self.osm_fields["type"]] == self.osm_fields["node"]:
                self.osmNodes.append(data)
            elif data[self.osm_fields["type"]] == self.osm_fields["way"]:
                self.osmWays.append(data)
            elif data[self.osm_fields["type"]] == self.osm_fields["relation"]:
                self.osmRelations.append(data)
        else:
            # end of xml; keys of all tags are in data as dictionary
            self.write(data)
            
        # print(data)
    
    def write(self, data):

        tags = data[0]
        datasetIds = {}

        print("Building OSM-Node CSV...")

        nodeFile = open("tmp/" + self.node_outfile, "w")
        nodeHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single),\tlat:Float(single),\tlon:Float(single)"] \
                                +[tag.replace(':','\:')+":String(single)" for tag in tags['node']]) + "\n"
        
        nodeFile.write(nodeHeader)

        for node in self.osmNodes:

            datasetid = str(math.floor(10*float(node[self.osm_fields["tags"]][self.osm_fields["lat"]]))/10) \
                + "N" + str(math.floor(10*float(node[self.osm_fields["tags"]][self.osm_fields["lon"]]))/-10) + "W"
            datasetIds[node[self.osm_fields["id"]]] = datasetid
            
            row = ",\t".join(
                ["n{},\t{},\t{},\t{},\t{},\t{}". \
                 format(node[self.osm_fields["id"]], self.osm_node_label, datasetid, 
                        node[self.osm_fields["id"]], node[self.osm_fields["tags"]][self.osm_fields["lat"]], 
                        node[self.osm_fields["tags"]][self.osm_fields["lon"]])] + \
                ['"{}"'.format(node[self.osm_fields["tags"]][tag].replace('"','""')) 
                 if tag in node[self.osm_fields["tags"]] 
                 else '' for tag in tags[self.osm_fields["node"]]]) + "\n"
            
            nodeFile.write(row)

        nodeFile.close()

        print("Building OSM-Way CSVs...")

        wayFile = open("tmp/" + self.way_outfile, "w")
        wayLinkFile = open("tmp/" + self.waylink_outfile, "w")

        wayHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single)"] + \
                               [tag.replace(':','\:')+":String(single)" for tag in tags[self.osm_fields["way"]]]) + "\n"
        wayLinkHeader = "~id,\t~label,\t~from,\t~to,\t__datasetid:String(single),\tway-id:String(single)\n"
        
        wayFile.write(wayHeader)
        wayLinkFile.write(wayLinkHeader)
        
        for way in self.osmWays:

            if way[self.osm_fields["nodes"]][0] in datasetIds:
                datasetid = datasetIds[way[self.osm_fields["nodes"]][0]]
            elif way[self.osm_fields["nodes"]][-1] in datasetIds:
                datasetid = datasetIds[way[self.osm_fields["nodes"]][-1]]
            else:
                datasetid = data[1]

            row = ",\t".join(
                ["w{},\t{},\t{},\t{}".format(way[self.osm_fields["id"]], 
                                             self.osm_way_label, 
                                             datasetid, 
                                             way[self.osm_fields["id"]])] + \
                ['"{}"'.format(way[self.osm_fields["tags"]][tag].replace('"','""')) 
                 if tag in way[self.osm_fields["tags"]] 
                 else '' for tag in tags[self.osm_fields["way"]]]) + "\n"
            
            wayFile.write(row)

            rowLink = "wf{0},\tFIRST,\tw{0},\tn{1},\t{2},\n".format(way[self.osm_fields["id"]], 
                                                                    way[self.osm_fields["nodes"]][0], 
                                                                    datasetid)
            wayLinkFile.write(rowLink)
            
            rowLink = "wl{0},\tLAST,\tw{0},\tn{1},\t{2},\n".format(way[self.osm_fields["id"]], 
                                                                   way[self.osm_fields["nodes"]][-1], 
                                                                   datasetid)
            wayLinkFile.write(rowLink)
            
            for i in range(len(way[self.osm_fields["nodes"]]) - 1):
                rowLink = "wr{0}-{1},\tWAY,\tn{2},\tn{3},\t{4},\t{0}\n".format(way[self.osm_fields["id"]], 
                                                                               i+1, 
                                                                               way[self.osm_fields["nodes"]][i], 
                                                                               way[self.osm_fields["nodes"]][i+1], 
                                                                               datasetid)
                wayLinkFile.write(rowLink)
        
        wayFile.close()
        wayLinkFile.close()

        print("Building OSM-Relation CSVs...")

        relationFile = open("tmp/" + self.relation_outfile, "w")
        relationLinkFile = open("tmp/" + self.relationlink_outfile, "w")

        relationHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single)"] + \
                                    [tag.replace(':','\:')+":String(single)" for tag in tags[self.osm_fields["relation"]]]) + "\n"

        relationLinkHeader = "~id,\t~label,\t~from,\t~to,\trole:String(single),\t__datasetid:String(single)\n"
        relationFile.write(relationHeader)
        relationLinkFile.write(relationLinkHeader)
        
        for relation in self.osmRelations:

            row = ",\t".join(
                ["r{0},\t{2},\t{1},\t{0},\t".format(relation[self.osm_fields["id"]], 
                                                             data[1],
                                                             self.osm_relation_label)] + \
                ['"{}"'.format(relation[self.osm_fields["tags"]][tag].replace('"', '""')) 
                 if tag in relation[self.osm_fields["tags"]] 
                 else '' for tag in tags[self.osm_fields["relation"]]]) + "\n"
            
            relationFile.write(row)
            
            for i in range(len(relation[self.osm_fields["members"]])):
                
                rowLink = "rm{0}-{1},\tMEMBER,\tr{0},\t{2}{3},\t{4},\t{5}\n".format(relation[self.osm_fields["id"]], i+1, 
                                                                                    relation[self.osm_fields["members"]][i][self.osm_fields["type"]][0],
                                                                                    relation[self.osm_fields["members"]][i][self.osm_fields["ref"]],
                                                                                    relation[self.osm_fields["members"]][i][self.osm_fields["role"]],
                                                                                    datasetid)
                relationLinkFile.write(rowLink)
        
        relationFile.close()
        relationLinkFile.close()