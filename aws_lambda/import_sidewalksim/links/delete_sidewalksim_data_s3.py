"""
The script contains functions to delete SidewalkSim data in json files on the AWS S3
based on the input data_set_id.

"""

import boto3


class SidewalkSimAWSDataDeleteS3:

	def __init__(self, data_set_id, sidewalk_bucket, prefix=None):

		self.data_set_id = data_set_id
		self.s3 = boto3.client("s3")
		
		# name the bucket and the folder where to lookup objects
		self.bucket = sidewalk_bucket

		if prefix is None:
			self.prefix = data_set_id + "/" # default to folder data_set_id/ in S3
		else:
			self.prefix = prefix # e.g. /tmp/data_set_id/ folder in S3

		
	def get_objects(self):
		
		# get objects inside the bucket and prefix specified
		self.objects = self.s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)   


	def extract_objects(self):

		# extract only the object names from the objects
		self.object_files = []

		if "Contents" in self.objects.keys():
			contents = self.objects["Contents"]

			for content in contents:

				object_save = {}
				object_name = content["Key"]
				object_save["Key"] = object_name

				self.object_files.append(object_save)

		if self.object_files:
			has_objects = True
		else:
			has_objects = False

		return has_objects

	
	def delete_objects(self):
		
		response = dict()
		
		if self.object_files:
			# delete objects inside the bucket and prefix specified
			response = self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": self.object_files})

		return response


	def delete_folder(self):

		# delete folder once all objects inside the folder are deleted
		self.s3.delete_object(Bucket=self.bucket, Key=self.prefix)
