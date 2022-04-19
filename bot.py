import time, csv, requests, aylien_news_api, pygsheets, os, re
import pandas as pd
from datetime import datetime
from aylien_news_api.rest import ApiException
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

class Bot:
    ''' PARAMS '''

    BUCKETS = {
        'New Product': [
            'ay.biz.newprod',
            'ay.biz.pdev',
            ],
        'Acquisition': [
            'ay.biz.manda',
            'ay.fin.spac'
            ],
        'Funding': [
            'ay.fin.corpfund',
            'ay.fin.private',
            'ay.fin.grants',
            'ay.fin.persinv',
            'ay.fin.pefund',
            'ay.biz.majann'
            ],
        'IPO': [
            'ay.fin.offering',
            'ay.pol.govpriv',
            'ay.fin.short',
            'ay.fin.imbalanc',
            'ay.biz.majority'
            ],
        'New Hire': [
            'ay.biz.recruit',
            'ay.biz.brdmove',
            'ay.biz.corpgov',
            'ay.biz.execpers',
            'ay.lifesoc.persmove',
            'ay.lifesoc.hiring'
            ],
        'Earnings Reports': [
            'ay.fin.reports',
            'ay.biz.annmtg',
            'ay.biz.earnpre'
            ],
        'New Partnership': [
            'ay.biz.newchan',
            'ay.biz.jointven',
            'ay.biz.stratall'
            ],
        'Lawsuit': [
            'ay.biz.events',
            'ay.biz.crime',
            'ay.biz.litigate',
            'ay.lifesoc.cyber',
            'ay.lifesoc.litigate'
            ],
        'misc': [
            'ay.biz.markfore',
            'ay.biz.changes',
            'ay.biz.emerging',
            'ay.biz.announce',
            'ay.biz.intprop',
            'ay.biz.out',
            'ay.biz.salmark',
            'ay.biz.sectors',
            'ay.biz.supplych',
            'ay.econ.area',
            'ay.econ.analysis',
            'ay.econ.dev',
            # 'ay.econ.sectors',
            'ay.fin.agreemnt'
            'ay.fin.charehld',
            'ay.fin.porfol',
            'ay.fin.markets',
            # 'ay.gen',
            'ay.bir.awards',
            'ay.econ.labor',
            'ay.biz.strikes',
            'ay.biz.layoffs',
            'ay.biz.philan',
            'ay.spec.events',
            'ay.spec.headline',
            'ay.lifesoc.briefs',
            'ay.appsci.spec',
        ]
    }

    core_key = os.environ['CORE_KEY']
    core_url = os.environ['CORE_URL']
    core_field_list = [
        "downloadUrl",
        "title",
        "abstract",
        "yearPublished",
        "authors"
    ] # Full list here - https://api.core.ac.uk/docs/v3#operation/null

    semantic_key = os.environ['SEMANTIC_KEY']
    semantic_url = os.environ['SEMANTIC_URL']
    semantic_field_list = [
        # "externalIds",
        "url",
        "title",
        "abstract",
        # "venue",
        "year",
        # "referenceCount",
        # "citationCount",
        # "influentialCitationCount",
        # "isOpenAccess",
        # "fieldsOfStudy",
        "authors"
    ] # The full list is here. Uncommenting fields will break the combine_papers function

    paper_limit = os.environ['PAPER_LIMIT']
    parent_id = os.environ['PARENT_FOLDER_ID']

    configuration = aylien_news_api.Configuration()

    # Configure API key authorization: app_id
    configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = os.environ['AYLIEN_APP_ID']
    
    # Configure API key authorization: app_key
    configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = os.environ['AYLIEN_APP_KEY']
    
    # Defining host is optional and default to https://api.aylien.com/news
    configuration.host = os.environ['AYLIEN_HOST']

    def __init__(self, channel, query, secondaries):
        self.channel = channel
        self.query = query
        self.secondary_keywords = secondaries

    def get_message_payload(self, research_link, news_link):
        """Compose a message with the links to the google sheets that include the query results.
        Slack has a slightly different markdown formatter so note the link structure.
        """
        message = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    '• :chart_with_upwards_trend: <https://docs.google.com/spreadsheets/d/'+research_link+'|research>\n'+
                    '• :newspaper: <https://docs.google.com/spreadsheets/d/'+news_link+'|news>'
                ),
            },
        }
        return {
            "channel": self.channel,
            "blocks": [
                message,
            ],
        }

    def _semantic_query (self, q):
        """Query semantic scholar.
        """
        params = {
            'query': q,
            'fields': ",".join(self.semantic_field_list),
            'limit': self.paper_limit
        }
        headers = {
            'Authorization': self.semantic_key
        }
        response = requests.get(self.semantic_url, params=params, headers=headers)
        return response.json()['data']
    
    def _core_query (self, q):
        """Query core API.
        """
        params = {
            'q': q,
            'exclude': 'fullText',
            'limit': self.paper_limit,
            'sort': 'relevance'
        }
        headers={"Authorization":"Bearer "+self.core_key}
        
        response = requests.get(self.core_url, params=params, headers=headers)
        return response.json()['results']
    
    def _if_space (self, word):
        """When a query word as a space, we want to treat that as a phrase.
        NOTE - Currently not in use
        """
        if " " in word:
            return f'((title: "{word}") OR (abstraction: "{word}"))'
        else:
            return f'((title: {word}) OR (abstraction: {word}))'
    
    def _format_core_query (self, query, secondary_keywords):
        """The CORE query structure as seen in the backend code from the India team.
        NOTE - Currently not in use
        """
        q = query[1:len(query)-1] if query[0] == '(' and query[len(query)-1] == ')' else query
        
        andArr = []
        for word in q.lower().split(' and '):
            if ('or' in word.lower()):
                orArr = []
                word = word[1:len(word)-1] if word[0] == '(' and word[len(word)-1] == ')' else word
                for w in word.lower().split(' or '):
                    orArr.append(ifSpace(w))
                word = "(" + " OR ".join(orArr) + ")"
            else:
                word = ifSpace(word)
            andArr.append(word)
        q = " AND ".join(andArr)
        
        temp_array = []
        for i, word in enumerate(secondary_keywords):
            temp_array.append(ifSpace(word))
    
        q += " AND (" + " OR ".join(temp_array) + ")"
        q = f'({q})'
        return q
    
    def _format_generic_query (self, query, secondary_keywords):
        """This adds secondary keywords to the original query.
        NOTE - Currently not in use
        """
        q = query[1:len(query)-1] if query[0] == '(' and query[len(query)-1] == ')' else query
        
        temp_array = []
        for i, word in enumerate(secondary_keywords):
            if " " in word:
                temp_array.append(f'"{word.lower()}"')
            else:
                temp_array.append(word.lower())
    
        q += " AND (" + " OR ".join(temp_array) + ")"
        q = f'({q})'
        return q

    def _replace_ands_ors(self, q):
        for s in ['or', 'Or', 'oR']:
            q = q.replace(s, 'OR')
        for s in ['and', 'And', 'aNd', 'anD', 'ANd', 'aND']:
            q = q.replace(s, 'AND')
        return q
    
    def combine_papers (self):
        """Combine research paper API results into pandas dataframes and concatenate.
        Also, find which secondary keywords show up in the title and abstract.
        """

        # Parse semantic results
        semantic_pull = pd.json_normalize(self._semantic_query(self._replace_ands_ors(self.query)))
        semantic_pull = semantic_pull.drop(['paperId'], axis = 1)
        semantic_pull['authors'] = semantic_pull['authors'].map(lambda x: [i['name'] for i in x])
        
        # Parse core results
        core_pull = pd.json_normalize(self._core_query(self._replace_ands_ors(self.query)))
        core_pull_filtered = core_pull[self.core_field_list].copy()
        core_pull_filtered['authors'] = core_pull_filtered['authors'].map(lambda x: [i['name'] for i in x])
        core_pull_filtered.rename(columns={'downloadUrl': 'url', 'yearPublished': 'year'}, inplace=True)
        
        # Clean up
        concat_frames = pd.concat([semantic_pull, core_pull_filtered])
        no_duplicates = concat_frames[~concat_frames.duplicated(subset='title', keep='first')]
        no_duplicates = no_duplicates.reset_index()
        no_duplicates = no_duplicates.drop(['index'], axis=1)
        
        # Find secondary keywords
        secondary_array = []
        for index, row in no_duplicates.iterrows():
            temp = []
            for word in self.secondary_keywords:
                title = '' if not row['title'] else row['title'].lower()
                abstract = '' if not row['abstract'] else row['abstract'].lower()
                if word.lower() in title or word.lower() in abstract:
                    temp.append(word.lower())
            secondary_array.append(temp)
        no_duplicates['secondary'] = secondary_array
            
        return no_duplicates

    def _get_search_opts(self, query, category, days_behind=90, aql=True, per_page=25):
        """Helper function for the get_news function. Creates the payload for the Aylien API.
        """
        data = {
            'language': ['en'],
            'published_at_end': 'NOW',
            'sort_by': 'relevance',
            'cursor': '*',
            'per_page': per_page,
        }
        data['published_at_start'] = ''.join(['NOW-', str(days_behind), 'DAYS'])
        data['text'] = query
        if aql:
            data['aql'] = \
                ''.join([
                    'categories: {{taxonomy: aylien AND id: ',
                    category,
                    ' AND score: [0.7 TO *]}}',
                ])
        return data

    def get_news (self):
        """Query Aylien API for news and collect into pandas dataframe.
        """

        # Create an instance of the API class
        api_instance = aylien_news_api.DefaultApi(aylien_news_api.ApiClient(self.configuration))

        collected_data  = {
            'category': [],
            'title': [],
            'source': [],
            'country': [],
            'links': [],
        }
        for key in self.BUCKETS.keys():
            if key == 'misc':
                continue
            try:
                # List Stories
                api_response = api_instance.list_stories(**self._get_search_opts(self.query, '(' + ' OR '.join(self.BUCKETS[key]) + ')'))

                for story in api_response.stories:
                    if len(story.source.locations) > 0:
                        country = story.source.locations[0].country
                    else:
                        country = ''

                    collected_data['category'].append(key)
                    collected_data['title'].append(story.title)
                    collected_data['source'].append(story.source.name)
                    collected_data['country'].append(country)
                    collected_data['links'].append(story.links.permalink)

            except ApiException as e:
                print("Exception when calling DefaultApi->list_stories: %s\n" % e)
            time.sleep(1)

        news_df = pd.DataFrame.from_dict(collected_data)
        noDuplicates = news_df[~news_df.duplicated(subset='title', keep='first')]
        noDuplicates = noDuplicates.reset_index()
        noDuplicates = noDuplicates.drop(['index'], axis=1)

        return noDuplicates

    def _create_spreadsheet(self, title, service):
        """Utilize Google drive API to create a new google sheet.
        """
        spreadsheet_data = {
            'properties': {'title': title}
        }

        return service.spreadsheets().create(body=spreadsheet_data,
                                            fields='spreadsheetId').execute()

    def _move_file(self, folder_id, file_id, drive_service):
        """Utilize Google drive API to move file into the folder specified by the environment var PARENT_FOLDER_ID.
        """
        # Retrieve the existing parents to remove
        file = drive_service.files().get(fileId=file_id,
                                         fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        # Move the file to the new folder
        file = drive_service.files().update(fileId=file_id,
                                            addParents=folder_id,
                                            removeParents=previous_parents,
                                            fields='id, parents').execute()

    def _share_folder(self, folder_id, service):
        """Share folder with user entered username (from slack modal).
        """
        permission = {
            'type': 'anyone',
            'role': 'writer',
        }
        service.permissions().create(
            fileId = folder_id,
            body=permission,
            fields='id'
        ).execute()

    def _create_folder(self, service):
        """Create a new folder to put the research and news files in.
        """
        file_metadata = {
            'name': datetime.now().strftime("%m/%d/%Y") + " | " + self.query,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.parent_id]
        }
        folder = service.files().create(body=file_metadata,
                                    fields='id').execute()

        return folder.get('id')

    def to_google(self, news_df, research_df):
        """Move data into google sheets and organize in folders correctly.
        """
        scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']

        # Authorize all relevant credentials
        creds = Credentials.from_authorized_user_file('google-credentials.json', scope)
        service = build('drive', 'v3', credentials=creds)
        spreadsheet_service = build('sheets', 'v4', credentials=creds)
        pyg = pygsheets.authorize(custom_credentials=creds)

        folder_id = self._create_folder(service)

        # Create two spreadsheets. One for research and the other for news
        links = []
        for item in [("research", research_df), ("news", news_df)]:
            spreadsheet = self._create_spreadsheet(item[0], spreadsheet_service)
            spread_id = spreadsheet.get('spreadsheetId')

            links.append(spread_id)

            worksheet = pyg.open_by_key(spread_id)[0]
            worksheet.set_dataframe(item[1], (0,0))

            self._move_file(folder_id, spread_id, service)

        self._share_folder(folder_id, service)

        return links
