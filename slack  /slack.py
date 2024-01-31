import requests
from slack_sdk import WebClient

#Create a client
token = "xoxb-<...>"
slack_client = WebClient(token=token)

#Join the channel
channel_id = "<...>"  #Replace with the actual channel ID
slack_client.conversations_join(channel=channel_id)

#Download the file from Google Cloud Storage
file_url = "https://storage.googleapis.com/your-bucket/connect-four-summary.csv"
response = requests.get(file_url)
file_contents = response.content

#Send a message and file
slack_client.files_upload(
    file=file_contents,
    channels=[channel_id],
    title='Connect Four Performance Summary',
    initial_comment='Thank you for your participation and enthusiasm throughout the games. Enjoy reviewing your performance and congratulations on your achievements!'
)
