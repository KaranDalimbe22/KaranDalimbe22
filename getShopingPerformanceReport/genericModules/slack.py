import os, sys
import requests
import json
import pandas as pd

sys.path.append(os.environ['SHARED_PACKAGE_PATH'])
from genericModules.utility import Utility

class Slack:

    def __init__(self):

        utility = Utility(__file__, cred_path='BASE')
        self.slack_bot_api_token = self.get_credentials(utility)
        self.api_url = "https://slack.com/api/"

    def get_user_id(self, email_id: str):

        """ 
            Gets the user_id of the user in workspace.
            user_id can be used to send the message to a 
            specific user.
            Pass user_id as a value to channel in send_message()
        
        """
        api_url = f"{self.api_url}/users.list"

        slack_bot_api_token = self.slack_bot_api_token

        params = {
            "token": slack_bot_api_token,
        }
        response = requests.get(api_url, params=params).json()

        members_df = pd.DataFrame(data=response['members'])
        members_df['email_id'] = members_df['profile'].apply(
            lambda row: row['email'] if 'email' in row else "")

        user_id = members_df[members_df['email_id'] == email_id].id

        return user_id.values[0]

    def get_channel_id(self, channel_name: str, channel_type: str):

        """ 
            Gets the channel id of a channel based on channel type
            Note:To get the channel_id of a private channel,
                make sure to add the App in channel first.

        """
        
        api_url = f"{self.api_url}/conversations.list"
        slack_bot_api_token = self.slack_bot_api_token

        params = {
            "token": slack_bot_api_token,
            "types": channel_type
        }

        response = requests.get(api_url, params=params).json()
        print(response)
        channels_df = pd.DataFrame(data=response['channels'])

        channel_id = channels_df[channels_df['name'] == channel_name].id

        return channel_id.values[0]

    def send_message(self, channel_id: str, message_text="", attachments=None):
        """ 
            Parameters:

            channel_id: id to which the msg is to be sent,
                        pass user_id if the msg is to be sent to specific user,
                        or pass channel_id if you want to send msg to channel.
                        user_id (returned from get_user_id() )
                        channel_id (returned from get_channel_id() )
            
            message_text: The text content of Message.
                          By default set to "", if only attachments are to be sent.

            attachments: Accepts list of dict if attachments are to be added,
                         None by default if not to be added.
        """
        api_url = f"{self.api_url}/chat.postMessage"
        params = {
            "token": self.slack_bot_api_token,
            "channel": channel_id,
            "text": message_text
        }
        if attachments is not None:
            params["attachments"] = json.dumps(attachments)

        response = requests.get(api_url, params=params).json()

        print(response)

    def get_credentials(self, utility):

        cred_file_dir = utility.get_credentials_folder_path()
        slack_json = 'slack_credentials.json'
        slack_cred_file = os.path.join(cred_file_dir, slack_json)
        with open(slack_cred_file) as f:
            data = json.load(f)
            keywordio_bot_access_token = data['Bot_User_OAuth_Access_Token']

        return keywordio_bot_access_token

def main():

    slack = Slack()	
	
    slack_user_id = slack.get_user_id(email_id="ameya@keywordio.com")
    print(slack_user_id)

    slack_channel_id = slack.get_channel_id(channel_name="test",channel_type="private_channel")
    print(slack_channel_id)

    message_text = 'Disapproved Products Spreadsheet Link'
    attachments_list=[
        {
            "pretext": 'Total Number of Disapproved Products',
            "text": "Blue Air 50 out of 100 (50%)"
        },
        {
            "title": 'Total Number of Disapproved Products',
            "text": "Billig Teknik 50 out of 100 (50%)"
        }
    ]

    # slack.send_message(channel_id=slack_user_id,message_text=message_text,attachments=attachments_list)

if __name__ == '__main__':
	main()
