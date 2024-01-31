from slack_sdk import WebClient

#Create a client
token = "xoxb-<...>"
slack_client = WebClient(token=token)

#Get list of channels
channels = slack_client.conversations_list()
channel_id = None

#Find the channel ID based on the channel name
for channel in channels['channels']:
    if channel['name'] == 'game-results':
        channel_id = channel['id']
        break

if channel_id:
    print("Channel ID: ", channel_id)
else:
    print("Channel not found.")
