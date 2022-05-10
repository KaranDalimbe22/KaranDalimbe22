# Standard library imports
import os
import sys
import json

class ReadJsons:

	@staticmethod
	def get_language_codes(self):
		"""Gets file name and returns the json file"""

		file_name = "language_codes.json"

		base_directory = os.path.dirname(__file__)

		file_path = os.path.join(base_directory,file_name)

		with open(file_path, "r") as file:
			language_codes_mapping = json.load(file)

		return language_codes_mapping