# Standard library imports
import base64
import os

# Third party imports
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import mimetypes
from apiclient import errors

#Implement bcc, remove sender
class Gmail:
	"""Google Mail class version"""

	def __init__(self, service:object):
		"""Parameterised Constructor"""

		self.service = service

	def send(self, email_data:dict):
		"""Send email after creating attachments to respective receiver.

		Args:
			email_data: email data dictionary containing to, subject,
			cc, bcc, message_text, attachment_folder_path, attachments and html

		Returns:
		A message object of mail sent.

		optional parameters like to, subject, message_text are taken from file 
		named 'email_data.txt' which must be stored in 'CREDENTIALS' folder.
		whereas html parameter is optional but must be set to True 
		if required type of email is HTML.
		"""
		try:
			print('Creating mail..')
			message = self.create_message(email_data)
			user_id = "me"

			self.service.users().messages().send(userId=user_id, 
				body=message).execute()
			print('Mail sent!')
		except errors.HttpError as error:
			print('An error occurred: %s' % error)

	def create_message(self, email_data:dict) -> dict:
		"""Create attachments to respective email.

		Args:
			email_data: email data dictionary containing to, subject,
			cc, bcc, message_text, attachment_folder_path, attachments and html

		Returns:
			An encoded attachment (dictionary) to be sent in the email.
		"""

		email_body = self._validate_email_data(email_data)
	
		message = MIMEMultipart()
		message['to'] = email_body['to']
		message['cc'] = email_body['cc']
		message['bcc'] = email_body['bcc']
		message['subject'] = email_body['subject']
		message_text = email_body['message_text']
		html = email_body['html']
		attachment_folder_path = email_body['attachment_folder_path']
		attachments = email_body['attachments']

		if html:
			msg = MIMEText(message_text,'html')
		else:
			msg = MIMEText(message_text)

		message.attach(msg)

		if attachments != []:
			for filename in attachments:      # attaching files to message
				file_path = os.path.join(attachment_folder_path, filename)
				attachment = MIMEApplication(open(file_path,'rb').read(), _subtype='txt')
				attachment.add_header('Content-Disposition', 'attachment', filename=filename)
				message.attach(attachment)


		return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

	def _validate_email_data(self, email_data:dict) -> dict:
		"""Validations for email data.

		Args:
			email_data: email data dictionary containing to, subject,
			cc, bcc, message_text, attachment_folder_path, attachments and html

		Returns:
			A final dictionary to be sent.
		"""

		email_body = {
			"to":'', #'abc@gmail.com'
			"cc":'', #'xyz@gmail.com,xyz@gmail.com'
			"bcc":'', #'123@gmail.com,xyz@gmail.com'			    
			"subject":'', 
			"attachment_folder_path" :'', 
			"attachments": [],
		}

		if email_data['to']:
			email_body['to'] = email_data['to']
		if email_data['cc']:
			email_body['cc'] = email_data['cc']
		if email_data['bcc']:
			email_body['bcc'] = email_data['bcc']
		if email_data['subject']:
			email_body['subject'] = email_data['subject']	
		if email_data['attachment_folder_path']:
			email_body['attachment_folder_path'] = email_data['attachment_folder_path']
		if email_data['attachments']:
			email_body['attachments'] = email_data['attachments']
		
		email_body['message_text'] = email_data['message_text']
		email_body['html'] = email_data['html']

		return email_body
		
		
			


  