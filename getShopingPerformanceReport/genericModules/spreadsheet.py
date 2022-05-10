class Spreadsheet:
	"""Class to perform spreadsheet operations such as
	create, read, write, update, format.
	"""

	def __init__(self, service:object, url:str='', value_input_option:str='RAW'):
		"""Parameterized  constructor with argument"""	

		self.service = service
		self.__url = url
		self.__id = self.get_id_from_url()
		self.value_input_option = value_input_option

	@classmethod
	def create(cls, service:object, title_name:str, sheet_name:str) -> object:
		"""Creates a new spreadsheet with title name and sheet name"""

		spreadsheet_body = {
			"properties": {
				"title": str(title_name),
			},
			"sheets":{
				"properties": {
					"title": str(sheet_name),
				},
			}
		}
		request = service.spreadsheets().create(body=spreadsheet_body)
		response = request.execute()

		url = response['spreadsheetUrl']

		spreadsheet = cls(service, url=url)

		return spreadsheet

	@property
	def url(self) -> str:
		"""Getter for spreadsheet id"""
		
		return self.__url

	@property
	def id_(self) -> str:
		"""Getter for spreadsheet id"""
		
		return self.__id

	def get_id_from_url(self) -> str:
		"""Splits the url by forward slash and loops over the split 
		list count of d element in the url is found for the id.
		"""		
		spreadsheet_id = ''

		if self.url != '':
			splited_data = self.url.split('/')	 
			count = 0
			for element in splited_data:
				count = count+1
				if str(element) == 'd':
					break			
			spreadsheet_id = splited_data[count]

		return spreadsheet_id	

	def add_sheet(self, sheet_name:str) -> int:
		"""Adds new sheet with sheet name to the existing spreadsheet"""

		request_body = {
			'requests': [{
				'addSheet': {
					'properties': {
						'title': str(sheet_name),
					}
				}

			}]
		}
		response = self.service.spreadsheets().batchUpdate(spreadsheetId=
			self.id_, body=request_body).execute()
		sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']

		return sheet_id

	def delete_sheet(self, sheet_id:int):
		"""Deletes sheet with sheet id from the existing spreadsheet"""

		body = {
			'requests': [{
				"deleteSheet": {
					"sheetId": int(sheet_id),
				}
			}]
		}
		self.service.spreadsheets().batchUpdate(spreadsheetId=self.id_, body=body).execute()

	def read(self, sheets_data:dict) -> dict:
		"""Reads from sheet with multiple combinations of sheet names 
		and range values.

		Combinations are single sheet with multiple ranges,
		multiple sheets with single range,
		multiple sheets with multiple ranges,
		single sheet name and single range value.

		Eg:
		Input:
			sheets_data =  {
				sheet_name:[range_value_1, range_value_2]
			}
		Returns:
			sheets_data = {
				sheet_name:{
					range_value:[data_list],
				}
			}
		"""
		result_values = {}

		for sheet_name, sheet_ranges in sheets_data.items():
			result_values[sheet_name] = {}
			values_data = result_values[sheet_name]
			for range_value in sheet_ranges:
				data = f'{sheet_name}!{range_value}'

				result = self.service.spreadsheets().values().get(
					spreadsheetId=self.id_, range=data).execute()

				values = result.get('values', [])
				values_data[range_value] = values

		return result_values

	def write(self, sheets_data:dict):
		"""Write to sheet with multiple combinations of sheet names, 
		range values and result values.

		Combinations are single sheet with multiple ranges,
		multiple sheets with single range,
		multiple sheets with multiple ranges,
		single sheet name and single range,

		Eg:
		Input:
			sheets_data = {
				sheet_name:{
					range_value:[data_list],
				}
			}
			
		"""
		data = []

		for sheet_name, range_values in sheets_data.items():
			for sheet_range, value in range_values.items():
				data.append({
					'range': f'{sheet_name}!{sheet_range}', 
					'values': value
				})

		body = {
			'valueInputOption': self.value_input_option,
			'data': data
		} 
		self.service.spreadsheets().values().batchUpdate(
			spreadsheetId=self.id_, body=body).execute()

	def append(self, sheets_data:dict):
		"""Appends rows to sheet with multiple combinations of sheet names, 
		range values and result values.

		Combinations are single sheet with multiple ranges,
		multiple sheets with single range,
		multiple sheets with multiple ranges,
		single sheet name and single range,

		Eg:
		Input:
			sheets_data = {
				sheet_name:{
					range_value:[data_list],
				}
			}
			
		"""	

		for sheet_name, range_values in sheets_data.items():
			for sheet_range, value in range_values.items():
				sheet_range_value = f'{sheet_name}!{sheet_range}'
				body = {
					"majorDimension": "ROWS",
					'values': value
				}
				self.service.spreadsheets().values().append(spreadsheetId=self.id_, range=sheet_range_value, valueInputOption=self.value_input_option, body=body).execute()

	def get_sheet_names_and_ids(self) -> list:
		"""Gets dictionary of all sheet ids with names"""

		sheet_metadata = self.service.spreadsheets().get(spreadsheetId= 
			self.id_).execute()
		
		sheet_properties = sheet_metadata.get('sheets')
		data = {}
		for sheet in sheet_properties:
			data[sheet.get("properties", {}).get("title", "")] = sheet.get("properties", {}).get("sheetId", "")
		
		return data 

	def update_sheet_name(self, sheet_id:int, new_sheet_name:str):
		"""Updates old sheet name to a new sheet name using its sheet id"""

		body = {
			"requests": [
				{
					"updateSheetProperties": {
						"properties": {
							"sheetId": int(sheet_id),
							"title": new_sheet_name,
						},
						"fields": "title",
					}
				}
			]
		}
		self.service.spreadsheets().batchUpdate(spreadsheetId=
			self.id_, body=body).execute()
		
	def clear_sheet(self, sheet_name:str, range_value:str):
		"""Clears a sheet with specific sheet name and range"""

		range_name = f'{sheet_name}!{range_value}' # sheet1!A:H
		body = {}
		self.service.spreadsheets().values().clear(spreadsheetId=
			self.id_, range=range_name, body=body).execute()

	def conditional_formatting(self, requests:list):
		"""This function needs a list of dictionaries with standard
		spreadsheet conventions for formatting the sheet.
		"""
		body = {
			'requests': requests
		}
		self.service.spreadsheets().batchUpdate(spreadsheetId=self.id_, 
			body=body).execute()
