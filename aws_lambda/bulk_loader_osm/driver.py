"""
Main script driver which parses OpenStreetMap (OSM) data and send them
to a graph database for storage: AWS Bulk Loader.

"""

import xml.sax

from osm_sax_python import OsmDataHandler
from csv_writer import OsmAWSBulkLoadCSVWriter as AWSBulkLoad


def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


def main(infile, dataset_id="area-unset", ignore_tags=False):

    @coroutine
    def printer():
        while True:
            event = yield
            # print(event)

    # send data to AWS S3 for Bulk Load
    AWSBulkLoadObj = AWSBulkLoad()

    @coroutine
    def querywriter():
        while True:
            event = yield

            # send data to CSV Writer
            AWSBulkLoadObj.receive(event)
                

    # parse and send data to a graph database
    xml.sax.parse(infile, OsmDataHandler(printer(), querywriter(), dataset_id, ignore_tags))

    return

