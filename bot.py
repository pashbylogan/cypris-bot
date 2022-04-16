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
    ]

    paper_limit = os.environ['PAPER_LIMIT']
    parent_id = os.environ['PARENT_FOLDER_ID']

    configuration = aylien_news_api.Configuration()

    # Configure API key authorization: app_id
    configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = os.environ['AYLIEN_APP_ID']
    
    # Configure API key authorization: app_key
    configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = os.environ['AYLIEN_APP_KEY']
    
    # Defining host is optional and default to https://api.aylien.com/news
    configuration.host = os.environ['AYLIEN_HOST']

    # The constructor for the class. It takes the channel name as the a
    # parameter and sets it as an instance variable.
    def __init__(self, channel, query, secondaries, email):
        self.channel = channel
        self.query = query
        self.secondary_keywords = secondaries
        self.email_to_share = email

    # Craft and return the entire message payload as a dictionary.
    def get_message_payload(self, research_link, news_link):
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
        if " " in word:
            return f'((title: "{word}") OR (abstraction: "{word}"))'
        else:
            return f'((title: {word}) OR (abstraction: {word}))'
    
    def _format_core_query (self, query, secondary_keywords):
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
    
    def combine_papers (self):
        # Parse semantic results
        semantic_pull = pd.json_normalize(self._semantic_query(self.query))
        semantic_pull = semantic_pull.drop(['paperId'], axis = 1)
        semantic_pull['authors'] = semantic_pull['authors'].map(lambda x: [i['name'] for i in x])
        
        # Parse core results
        core_pull = pd.json_normalize(self._core_query(self.query))
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
        # Create an instance of the API class
        api_instance = aylien_news_api.DefaultApi(aylien_news_api.ApiClient(self.configuration))

        d = set()
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
                        country = 'DNE'

                    # Remove duplicates
                    if len(d) == 0:
                        d.add(story.title)
                    else:
                        match = False
                        for k in d:
                            if (story.title in d) or (k in story.title) or (story.title in k):
                                match = True
                        if not match:
                            d.add(story.title)
                            collected_data['category'].append(key)
                            collected_data['title'].append(story.title)
                            collected_data['source'].append(story.source.name)
                            collected_data['country'].append(country)
                            collected_data['links'].append(story.links.permalink)

            except ApiException as e:
                print("Exception when calling DefaultApi->list_stories: %s\n" % e)
            time.sleep(1)

        return pd.DataFrame.from_dict(collected_data)

    def _create_spreadsheet(self, title, service):
        spreadsheet_data = {
            'properties': {'title': title}
        }

        return service.spreadsheets().create(body=spreadsheet_data,
                                            fields='spreadsheetId').execute()

    def _move_file(self, folder_id, file_id, drive_service):
        # Retrieve the existing parents to remove
        file = drive_service.files().get(fileId=file_id,
                                         fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        # Move the file to the new folder
        file = drive_service.files().update(fileId=file_id,
                                            addParents=folder_id,
                                            removeParents=previous_parents,
                                            fields='id, parents').execute()

    def _check_email(self, email):
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if(re.fullmatch(regex, str(email))):
            return True
        else:
            return False

    def _share_folder(self, folder_id, service):
        if self._check_email(self.email_to_share):
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': self.email_to_share
            }
            service.permissions().create(
                fileId = folder_id,
                body=permission,
                fields='id'
            ).execute()

    def _create_folder(self, service):
        file_metadata = {
            'name': datetime.now().strftime("%m/%d/%Y") + " | " + self.query,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [self.parent_id]
        }
        folder = service.files().create(body=file_metadata,
                                    fields='id').execute()

        return folder.get('id')

    def to_google(self, news_df, research_df):
        scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']

        # Authorize all relevant credentials
        creds = Credentials.from_authorized_user_file('google-credentials.json', scope)
        service = build('drive', 'v3', credentials=creds)
        spreadsheet_service = build('sheets', 'v4', credentials=creds)
        pyg = pygsheets.authorize(custom_credentials=creds)

        folder_id = self._create_folder(service)

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
