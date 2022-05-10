# Standard library imports
import io
import shutil
import os

# Third party imports
from googleapiclient.http import MediaIoBaseDownload

class Drive:
	"""Google Drive class version"""

	def __init__(self, service:object):
		"""Parameterised Constructor"""

		self.service = service
	
	def create(self, name:str, extension:str) -> str:
		"""Create any type of file or folder.

		Args:
			name: Name of the folder or file.
			extension: File extension to specify type of file or folder.

		Returns:
			The file or folder id.
		"""

		mime_type = self.get_mime_type(extension)
		file_metadata = {
			'name': name,
			'mimeType': mime_type
		}
		file = self.service.files().create(body=file_metadata, 
		fields='id').execute()

		return file['id']

	def get(self,  extension:str, name:str='', parent_id:str='') -> dict:
		"""Gets any type of file or folder.

		Args:
			extension: File extension to specify type of file or folder.
			
		Kwargs:
			name: Name of the folder or file.
			parent_id: Id of the parent folder.

		Returns:
			The dictionary of file/folder id , name and size.
		"""

		mime_type = self.get_mime_type(extension)
		data_dict = {}
		page_token = None

		q = "mimeType='{}'".format(mime_type)

		if parent_id == "" and name == "":
			query = q
		if name == "" and parent_id != "":
			query = q+' and "{}" in parents'.format(parent_id)
		if parent_id == "" and name != "":
			query = q+' and name contains "{}"'.format(name)
		if parent_id != "" and name != "":
			query = q+' and name contains "{}" and "{}" in parents'.format(name,parent_id)
		
		query = query+' and trashed=False'

		response = self.service.files().list(q=query,spaces='drive',
				fields='nextPageToken, files(id, name, size)',
				pageToken=page_token).execute()
		
		for file_data in response.get('files', []):
			data_dict[file_data.get('id')] = [file_data.get('name'),
				file_data.get('size')+' bytes']

		return data_dict

	def move(self, id_1:str, id_2:str):
		"""Moves any type of file or folder.

		Args:
			id_1: Id of the file/folder to move.
			id_2: Id of the folder to make as parent.

		Returns:
			None
		"""

		# Retrieve the existing parents to remove
		file = self.service.files().get(fileId=id_1, 
			fields='parents').execute()
		previous_parents = ",".join(file.get('parents'))

		# Move the file to the new folder
		file = self.service.files().update(fileId=id_1, addParents=id_2, 
			removeParents=previous_parents, fields='id, parents').execute()

	def trash(self, id_1:str):
		"""Move a file to the trash.

		Args:
			id_1: ID of the file to trash.

		Returns:
			The updated file if successful, None otherwise.
		"""
		
		trash_status = self.service.files().trash(fileId=id_1).execute()

		return trash_status	

	def upload(self, id_1:str,  extension:str, path_1:str):
		"""Uploads any type of file or folder.

		Args:
			id_1: Id of the file/folder to upload.
			extension: File extension to specify type of file or folder.
			path_1: Path of the file to upload.

		Returns:
			The file or folder id.
		"""
		
		name = os.path.split(path_1)
		mime_type = self.get_mime_type(extension)
		file_metadata = {
			'name': name,
			'parents': [id_1]
		}
		
		media = MediaFileUpload(path_1,
			mimetype=f'{mime_type}',resumable=True)
		file = self.service.files().create(body=file_metadata,
			media_body=media,fields='id').execute()

		return file.get('id')

	def download(self, id_1:str, download_path:str, extension:str=''):
		"""Downloads any type of file or folder.

		Args:
			id_1: Id of the file/folder to download.
			download_path: Path of the file to downlaod.
			extension: File extension to specify type of file or folder.

		Returns:
			None
		"""

		if extension != '' and 'google' in extension:
			mime_type = self.get_mime_type(extension)
			request = self.service.files().export_media(fileId=id_1,
												mimeType=f'{mime_type}')
		else:
			request = self.service.files().get_media(fileId=id_1)

		fh = io.BytesIO()
		downloader = MediaIoBaseDownload(fh, request)
		done = False
		while done is False:
			status, done = downloader.next_chunk()
		if done is True:
			fh.seek(0)
			with open(download_path, 'wb') as f:
				shutil.copyfileobj(fh, f, length=131072)
		else:
			print('file not downloaded')
	
	def get_mime_type(self, extension:str) -> str :
		"""Gets mime type of any extension.

		Args:
			extension: File extension to specify type of file or folder.

		Returns:
			The mimetype string as per the extension value.
		"""
		mime_type_dict = {
			'google_folder': 'application/vnd.google-apps.folder',
			'google_document': 'application/vnd.google-apps.document',
			'google_file': 'application/vnd.google-apps.file',
			'google_form': 'application/vnd.google-apps.form',
			'google_presentation': 'application/vnd.google-apps.presentation',
			'google_app_script': 'application/vnd.google-apps.script',
			'google_app_script_json': 'application/vnd.google-apps.script+json',
			'google_spreadsheet':'application/vnd.google-apps.spreadsheet',
			'html': 'text/html',
			'zip': 'application/zip',
			'txt': 'text/plain',
			'rtf': 'application/rtf',
			'sql':'text/sql',
			'odt': 'application/vnd.oasis.opendocument.text',
			'pdf': 'application/pdf',
			'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
			'epub': 'application/epub+zip',
			'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
			'ods': 'application/x-vnd.oasis.opendocument.spreadsheet',
			'csv': 'text/csv',
			'json':'application/json',
			'txt_tab_separated': 'text/tab-separated-values',
			'jpg': 'image/jpeg',
			'png': 'image/png',
			'svg': 'image/svg+xml',
			'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
			'odp': 'application/vnd.oasis.opendocument.presentation'
		}

		mime_type = mime_type_dict[extension]
		
		return mime_type

	def set_permissions(self, id_1:str, email_ids_list:str, 
		sendNotificationEmails:bool=False, role:str='read'):
		"""Adds permission any type of file or folder.

		Args:
			id_1: Id of the file/folder to download.
			email_ids_list: List of email ids to allow access.
		
		Kwargs:
			sendNotificationEmails: To allow access notifications.
			role: Type of access.

		Returns:
			None
		"""
		def callback(request_id, response, exception):
			"""Function required for creating batch request 
			in add_permissions function.
			"""

			if exception:
				# Handle error
				print(exception)
			else:
				print("Permission Id: %s" % response.get('id'))
		batch = self.service.new_batch_http_request(callback=callback)
		for email_id in email_ids_list:
			print(email_id,'-->',role)
			user_permission = {
				'type': 'user',
				'role': role,
				'emailAddress': email_id,
				'sendNotificationEmails' : sendNotificationEmails
			}
			batch.add(self.service.permissions().create(fileId=id_1,
				body=user_permission,fields='id'))
		batch.execute()


