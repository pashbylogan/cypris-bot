import os, json, threading
from flask import Flask, request, make_response
from slack import WebClient
from slack.errors import SlackApiError
from bot import Bot

# Initialize a Flask app to host the events adapter
app = Flask(__name__)

# Initialize a Web API client
slack_web_client = WebClient(token=os.environ.get("SLACK_TOKEN"))

def _send_modal(trigger_id):
    """Create a menu for the user to input structured data. Send it via the Slack API
    """
    message = {
	    "title": {
	    	"type": "plain_text",
	    	"text": "Custom Report Bot"
	    },
	    "submit": {
	    	"type": "plain_text",
	    	"text": "Submit"
	    },
	    "blocks": [
	    	{
	    		"type": "input",
	    		"element": {
	    			"type": "plain_text_input",
	    			"action_id": "query",
	    			"placeholder": {
	    				"type": "plain_text",
	    				"text": "Enter query here"
	    			}
	    		},
	    		"label": {
	    			"type": "plain_text",
	    			"text": "Query"
	    		}
	    	},
	    	{
	    		"type": "divider"
	    	},
	    	{
	    		"type": "input",
                "optional": True,
	    		"element": {
	    			"type": "plain_text_input",
	    			"action_id": "secondary",
	    			"placeholder": {
	    				"type": "plain_text",
	    				"text": "Enter list of words separated by commas (no spaces)"
	    			}
	    		},
	    		"label": {
	    			"type": "plain_text",
	    			"text": "Secondary Keywords"
	    		}
	    	},
	    	{
	    		"type": "divider"
	    	},
            {
                "block_id": "my_block_id",
                "type": "input",
                "optional": True,
                "label": {
                    "type": "plain_text",
                    "text": "Select a channel to post the result on",
                },
                "element": {
                    "action_id": "my_action_id",
                    "type": "conversations_select",
                    "default_to_current_conversation": True,
                    "response_url_enabled": True,
                },
            },
	    ],
	    "type": "modal"
    }

    slack_web_client.views_open(trigger_id=trigger_id, view=message)

def _send_processing_message(channel, query):
    """Let the user know we are computing results and include query for future reference.
    """
    # Post the onboarding message in Slack
    slack_web_client.chat_postMessage(channel=channel, text="Processing the following query: " + query)

def _create_csvs(channel, query, secondaries):
    """Craft the bot, get the API results, and send data to google
    """
    # Create a new bot object
    bot = Bot(channel, query, secondaries)

    # Research and news pandas dataframes
    research = bot.combine_papers()
    news = bot.get_news()
    patents = bot.get_patents()

    # Array of IDs for each file
    links = bot.to_google(news, research, patents)

    # Send user the relevant links
    slack_web_client.chat_postMessage(**bot.get_message_payload(*links))

@app.route('/slack/interact', methods=['POST'])
def interact():
    """Parse the event, and if /query was used send a modal.
    If model submission, process the input and run the bot.
    """

    if "command" in request.form:
        try:
            _send_modal(request.form['trigger_id'])
            return make_response("", 200)
        except SlackApiError as e:
            code = e.response["error"]
            return make_response('Failed to open a modal due to '+code, 200)

    if "payload" in request.form:
        payload = json.loads(request.form['payload'])
        if (
            payload["type"] == "view_submission"
        ):

            # Handle a data submission request from the modal
            submitted_data = payload["view"]["state"]["values"]
            channel_id =  payload["response_urls"][0]["channel_id"]
            secondary_keywords = []

            for key in submitted_data.keys():
                obj = submitted_data[key]
                if 'query' in list(obj.keys()):
                    query = obj['query']['value']
                elif 'secondary' in list(obj.keys()) and obj['secondary']['value']:
                    secondary_keywords = obj['secondary']['value'].split(',')

            # Simple message "Processing..." just to assure the user the task is working
            _send_processing_message(channel_id, query)

            # Need to start a thread because the modal needs to know asap if it successfully reached the endpoint
            thread = threading.Thread(target=_create_csvs, args=(channel_id, query, secondary_keywords))
            thread.start()

            # Close this modal with an empty response body
            return make_response("", 200)

    # If we make it here, an error occured
    return make_response("", 500)

@app.route('/', methods=['GET'])
def index():
    return make_response("I'm working!", 200)
