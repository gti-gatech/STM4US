import math

class OsmAWSBulkLoadCSVWriter:

    def __init__(self):
        self.osmNodes = []
        self.osmWays = []
        self.osmRelations = []
    def receive(self, data):
        if 'type' in data:
            if data['type'] == "node":
                self.osmNodes.append(data)
            elif data['type'] == "way":
                self.osmWays.append(data)
            elif data['type'] == "relation":
                self.osmRelations.append(data)
        else:
            # end of xml; keys of all tags are in data as dictionary
            self.write(data)
            
        # print(data)
    
    def write(self, data):
        tags = data[0]
        # datasetid = data[1]
        datasetIds = {}
        print("Building OSM-Node CSV...")
        nodeFile = open("tmp/node.csv", "w")
        nodeHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single),\tlat:Float(single),\tlon:Float(single)"] \
                                +[tag.replace(':','\:')+":String(single)" for tag in tags['node']]) + "\n"
        nodeFile.write(nodeHeader)
        for node in self.osmNodes:
            datasetid = str(math.floor(10*float(node['tags']['lat']))/10) + "N" + str(math.floor(10*float(node['tags']['lon']))/-10) + "W"
            datasetIds[node['id']] = datasetid
            # row = "n{},\tOSM-NODE,\t{},\t{},\t{},\t{},\t".\
            #     format(node['id'], datasetid, node['id'], node['tags']['lat'], node['tags']['lon']) + \
            #         ",\t".join('"{}"'.format(node['tags'][tag].replace('"','""')) \
            #                    if tag in node['tags'] else '' for tag in tags['node']) + "\n"
            row = ",\t".join(
                ["n{},\tOSM-NODE,\t{},\t{},\t{},\t{}".format(node['id'], datasetid, node['id'], node['tags']['lat'], node['tags']['lon'])] + \
                ['"{}"'.format(node['tags'][tag].replace('"','""')) if tag in node['tags'] else '' for tag in tags['node']]) + "\n"
            nodeFile.write(row)
        nodeFile.close()

        print("Building OSM-Way CSVs...")
        wayFile = open("tmp/way.csv", "w")
        wayLinkFile = open("tmp/wayLink.csv", "w")
        wayHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single)"] + \
                               [tag.replace(':','\:')+":String(single)" for tag in tags['way']]) + "\n"
        wayLinkHeader = "~id,\t~label,\t~from,\t~to,\t__datasetid:String(single),\tway-id:String(single)\n"
        wayFile.write(wayHeader)
        wayLinkFile.write(wayLinkHeader)
        for way in self.osmWays:
            if way['nodes'][0] in datasetIds:
                datasetid = datasetIds[way['nodes'][0]]
            elif way['nodes'][-1] in datasetIds:
                datasetid = datasetIds[way['nodes'][-1]]
            else:
                datasetid = data[1]
            row = ",\t".join(
                ["w{},\tOSM-WAY,\t{},\t{}".format(way['id'], datasetid, way['id'])] + \
                ['"{}"'.format(way['tags'][tag].replace('"','""')) if tag in way['tags'] else '' for tag in tags['way']]) + "\n"
            wayFile.write(row)
            rowLink = "wf{0},\tFIRST,\tw{0},\tn{1},\t{2},\n".format(way['id'], way['nodes'][0], datasetid)
            wayLinkFile.write(rowLink)
            rowLink = "wl{0},\tLAST,\tw{0},\tn{1},\t{2},\n".format(way['id'], way['nodes'][-1], datasetid)
            wayLinkFile.write(rowLink)
            for i in range(len(way['nodes']) - 1 ):
                rowLink = "wr{0}-{1},\tWAY,\tn{2},\tn{3},\t{4},\t{0}\n".format(way['id'], i+1, way['nodes'][i], way['nodes'][i+1], datasetid)
                wayLinkFile.write(rowLink)
        wayFile.close()
        wayLinkFile.close()

        print("Building OSM-Relation CSVs...")
        relationFile = open("tmp/relation.csv", "w")
        relationLinkFile = open("tmp/relationLink.csv", "w")
        relationHeader = ",\t".join(["~id,\t~label,\t__datasetid:String(single),\tid:String(single)"] + \
                                    [tag.replace(':','\:')+":String(single)" for tag in tags['relation']]) + "\n"

        relationLinkHeader = "~id,\t~label,\t~from,\t~to,\trole:String(single),\t__datasetid:String(single)\n"
        relationFile.write(relationHeader)
        relationLinkFile.write(relationLinkHeader)
        for relation in self.osmRelations:
            row = ",\t".join(
                ["r{0},\tOSM-RELATION,\t{1},\t{0},\t".format(relation['id'], data[1])] + \
                ['"{}"'.format(relation['tags'][tag].replace('"','""')) if tag in relation['tags'] else '' for tag in tags['relation']]) + "\n"
            relationFile.write(row)
            for i in range(len(relation['members'])):
                
                rowLink = "rm{0}-{1},\tMEMBER,\tr{0},\t{2}{3},\t{4},\t{5}\n".format(relation['id'], i+1, 
                                                                                    relation['members'][i]['type'][0],
                                                                                    relation['members'][i]['ref'],
                                                                                    relation['members'][i]['role'],
                                                                                    datasetid)
                relationLinkFile.write(rowLink)
        relationFile.close()
        relationLinkFile.close()