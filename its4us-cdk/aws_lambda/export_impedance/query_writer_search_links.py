"""
The script consists of openCypher queries for searching and filtering impedance links for a certain grid cell
based on the lat/lon of the links that is set up from query_writer_setup_locations.py on AWS Neptune database:
    "bolt://<neptune-endpoint>:8182"

If a certain grid cell is not found from the input, impedance links of whole study area based on __datasetid 
will be found.

"""

import io
import csv
import boto3

from graph_database_driver import GraphDatabaseDriver
from cypher_utils import identifier


class ImpedanceLinksSearchQuery:

    def __init__(self, PARAMS):

        self.s3 = boto3.client("s3")
        self.cols = PARAMS["IMPEDANCE_COLUMNS"] # column header of the impedance data to csv
        self.csv = PARAMS["IMPEDANCE_HEADERS"]
        self.search_area = PARAMS["DATASETID"] # coordinate grid name like 33.8N84.3W or search based on __datasetid
        self.s3_public_export_bucket = PARAMS["PUBLIC_EXPORT_BUCKET"]
        self.study_area = PARAMS["STUDY_AREA"]
        self.study_area_boundary = PARAMS["STUDY_AREA_BOUNDARY"]
        self.osm_node_label = PARAMS["OSM_NODE_LABEL"]
        self.impedance = PARAMS["IMPEDANCE"]
        self.impedance_link_attrs = PARAMS["IMPEDANCE_LINK_ATTRS"]
        self.export_impedance_fields = PARAMS["EXPORT_IMPEDANCE_FIELDS"]
        self.lambda_outfile = PARAMS["LAMBDA_OUTFILE"]
        self.s3_outfile = PARAMS["S3_OUTFILE"]
        
        self.driverObj = GraphDatabaseDriver(PARAMS)


    def search_links_query(self, query):
        export_fields = self.export_impedance_fields
        link_attrs = self.impedance_link_attrs
        fixed_identifiers = set(export_fields.values()) | set(link_attrs.values())

        query += "RETURN r.{} as {}, osm1.id as {}, ". \
            format(identifier(export_fields["Timestamp"], fixed_identifiers),
                   identifier(export_fields["Timestamp"], fixed_identifiers),
                   identifier(export_fields["Upstream Node"], fixed_identifiers))
        
        query += "osm2.id as {}, r.{} as {}, r.{} as {}, ". \
            format(identifier(export_fields["Downstream Node"], fixed_identifiers),
                   identifier(export_fields["Downstream Node"], fixed_identifiers),
                   identifier(link_attrs["stmAdaPathLinkID"], fixed_identifiers),
                   identifier(export_fields["Way Id"], fixed_identifiers),
                   identifier(link_attrs["stmAdaPathLinkLength"], fixed_identifiers),
                   identifier(export_fields["Link Length"], fixed_identifiers))
        
        query += ", ".join(
            f"r.{identifier(col)} as {identifier(col)}" for col in self.cols
        )

        return query
    
    def search_links_grid(self, search_area):

        results = list()

        # search impedance links in a grid specified based on the lat/lon of the links
        query = f"MATCH (osm1)-[r:{identifier(self.impedance)}]->(osm2) "
        query += "WHERE osm1.lat >= $min_lat AND osm1.lat <= $max_lat " \
                 "AND osm1.lon >= $min_lon AND osm1.lon <= $max_lon "
        
        query = self.search_links_query(query)

        #query += "RETURN r.Timestamp as Timestamp, osm1.id as `Upstream Node`, " \
        #    + "osm2.id as `Downstream Node`, r.stmAdaPathLinkID as `Way Id`, r.stmAdaPathLinkLength as `Link Length`, " \
        #    + ", ".join(["r.`{0}` as `{0}`".format(col) for col in self.cols])
        
        # execute the query
        results = self.driverObj.run_query(
            "CHECK",
            query,
            {
                "min_lat": search_area[self.study_area_boundary["min_lat"]],
                "max_lat": search_area[self.study_area_boundary["max_lat"]],
                "min_lon": search_area[self.study_area_boundary["min_lon"]],
                "max_lon": search_area[self.study_area_boundary["max_lon"]],
            },
        )

        return results
    
    def search_links_full(self, search_area):

        results = list()

        # export impedance links for the full study area based on __datasetid
        query = (
            f"MATCH (osm1:{identifier(self.osm_node_label)})"
            f"-[r:{identifier(self.impedance)}]->"
            f"(osm2:{identifier(self.osm_node_label)}) "
            f"WHERE r.{identifier(self.impedance_link_attrs['__datasetid'], {'__datasetid'})} = $dataset_id "
        )
        
        query = self.search_links_query(query)

        #query += "RETURN r.Timestamp as Timestamp, osm1.id as `Upstream Node`, " \
        #    "osm2.id as `Downstream Node`, r.stmAdaPathLinkID as `Way Id`, r.stmAdaPathLinkLength as `Link Length`, " \
        #    + ", ".join(["r.`{0}` as `{0}`".format(col) for col in self.cols])
        
        # execute the query
        results = self.driverObj.run_query("CHECK", query, {"dataset_id": search_area})

        return results
        
    
    def generate_location_search_query(self):

        try:
            # look up a certain grid cell to search the links
            search_area = self.study_area[self.search_area] # a certain grid cell
            message = "NOTE: Impedance links on grid cell {} will be exported.".format(self.search_area)
            grid_found = True
        except:
            # export the links based on __datasetid of the whole study area
            search_area = self.search_area # whole study area based on __datasetid
            message = "NOTE: Impedance links on full study area with {}: {} will be exported.". \
                format(self.impedance_link_attrs["__datasetid"], self.search_area)
            grid_found = False

        print(message)

        if grid_found:
            # search impedance links based on the input grid cell
            results = self.search_links_grid(search_area)
        else:
            # export impedance links based on __datasetid for full study area
            results = self.search_links_full(search_area)

        # save the search query result to csv data format
        for record in results:
            data = record.data()
            self.csv += ",".join(["{}".format(val) for val in data.values()]) + "\n"

        return

    
    def create_transaction(self):

        self.generate_location_search_query()

        # return the output csv data format
        #out_csv = bytes(self.csv, "utf-8")

        # use io.StringIO to treat the string data as a file
        string_io = io.StringIO(self.csv)

        # create a list of lists by reading the string
        reader = csv.reader(string_io)
        data_as_list = list(reader)

        # write the list of lists to a new CSV file
        # the 'newline=""' argument is important for proper CSV file handling on all platforms
        with open(self.lambda_outfile, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerows(data_as_list)

        self.s3.upload_file(self.lambda_outfile, self.s3_public_export_bucket, self.s3_outfile)

        message = "DONE uploading searched impedance links to S3 bucket: " + self.s3_public_export_bucket \
            + " in the file: " + self.s3_outfile
        print(message)

        return
