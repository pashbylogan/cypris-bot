import time, csv, requests, aylien_news_api, pygsheets, os, re, pycountry
import pandas as pd
from datetime import datetime
from aylien_news_api.rest import ApiException
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

class Bot:
    ''' PARAMS '''

    BUCKETS = {
        'New Product': [
            'ay.biz.newprod',
            'ay.biz.pdev',
            ],
        'Acquisition': [
            'ay.biz.manda',
            'ay.fin.spac' ],
        'Funding': [
            'ay.fin.corpfund',
            'ay.fin.private',
            'ay.fin.grants',
            'ay.fin.persinv',
            'ay.fin.pefund',
            'ay.biz.majann' ],
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

    cypris_url = os.environ['CYPRIS_URL']
    patent_field_list = [
        'country',
        'patentNumber',
        'publicationDate',
        'inventor',
        'assignee',
        'title',
        'abstraction',
        'documentType',
        'categoryId',
        'classificationText'
    ]

    paper_limit = os.environ['PAPER_LIMIT']
    patent_limit = os.environ['PATENT_LIMIT']
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

    def get_message_payload(self, research_link, news_link, patent_link):
        """Compose a message with the links to the google sheets that include the query results.
        Slack has a slightly different markdown formatter so note the link structure.
        """
        message = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    '• :chart_with_upwards_trend: <https://docs.google.com/spreadsheets/d/'+research_link+'|research>\n'+
                    '• :newspaper: <https://docs.google.com/spreadsheets/d/'+news_link+'|news>\n'
                    '• :closed_lock_with_key: <https://docs.google.com/spreadsheets/d/'+patent_link+'|patents>'
                ),
            },
        }
        return {
            "channel": self.channel,
            "blocks": [
                message,
            ],
        }

    ''' PAPER METHODS '''

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
    
    def _format_core_query (self, query):
        """The CORE query structure as seen in the backend code from the India team.
        """
        q = self._replace_ands_ors(query)

        parsed_q = q.replace(' AND ', ' ')
        parsed_q = parsed_q.replace(' OR ', ' ')
        parsed_q = parsed_q.replace(')', '')
        parsed_q = parsed_q.replace('(', '')
        parsed_q = re.sub('\s+',' ',parsed_q)

        individual_words = []
        individual_words.extend(re.findall(r'"(.*?)"', parsed_q, re.DOTALL))
        print('individual words 1', individual_words)
        for word in individual_words : parsed_q = parsed_q.replace('"'+word+'"', '')
        print('parsed q 1', parsed_q)
        individual_words.extend(parsed_q.split(' '))
        individual_words.remove('') if '' in individual_words else None
        individual_words.remove('""') if '""' in individual_words else None
        print('individual words 2', individual_words)
        q = q.replace('"', '')
        print('RUNNING QUERY', q)
        for word in individual_words:
            q = q.replace(word, os.environ['CORE_TEMPLATE_EXACT'].replace('KEYWORD', word))
            print('RUNNING QUERY', q)
        
        q = f'({q})'
        print('CORE QUERY', q)
        return q

    def _format_semantic_query (self, query):
        """The semantic query structure as seen in the API docs.
        NOTE - Boolean search is not an option at the time of writing.
        """
        query = query.replace('"', '')
        query = query.lower()
        query = query.replace(')', '')
        query = query.replace('(', '')
        query = query.replace(' and ', ' ')
        query = query.replace(' or ', ' ')
        query = re.sub('\s+',' ',query)
        words = query.split(' ')
        words = ' '.join(words)
        print('SEMANTIC QUERY', words)
        return words
    
    def _replace_ands_ors(self, q):
        for s in ['or', 'Or', 'oR']:
            q = q.replace(s, 'OR')
        for s in ['and', 'And', 'aNd', 'anD', 'ANd', 'aND', 'AnD']:
            q = q.replace(s, 'AND')
        return q
    
    def combine_papers (self):
        """Combine research paper API results into pandas dataframes and concatenate.
        Also, find which secondary keywords show up in the title and abstract.
        """

        # Parse semantic results
        semantic_pull = pd.json_normalize(self._semantic_query(self._format_semantic_query(self.query)))
        semantic_pull = semantic_pull.drop(['paperId'], axis = 1)
        semantic_pull['authors'] = semantic_pull['authors'].map(lambda x: [i['name'] for i in x])
        
        # Parse core results
        core_pull = pd.json_normalize(self._core_query(self._format_core_query(self.query)))
        print('LENGTH', len(core_pull))
        if (len(core_pull) > 0):
            core_pull_filtered = core_pull[self.core_field_list].copy()
            core_pull_filtered['authors'] = core_pull_filtered['authors'].map(lambda x: [i['name'] for i in x])
            core_pull_filtered['abstract'] = core_pull_filtered['abstract'].map(lambda x: re.sub('\s+',' ',str(x)))
            core_pull_filtered.rename(columns={'downloadUrl': 'url', 'yearPublished': 'year'}, inplace=True)
            concat_frames = pd.concat([semantic_pull, core_pull_filtered])
        else:
            concat_frames = semantic_pull
        
        # Clean up
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

    ''' NEWS METHODS '''

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
        print('NEWS QUERY', query)
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

    ''' PATENT METHODS '''

    def _patent_query(self, q):
        """ Create an API request for cypris' dev database
        """

        def util(q):
            q = q.replace('"', '')
            q = q.split(' AND ')
            for i, item in enumerate(q):
                item = item.replace('(', '')
                item = item.replace(')', '')
                item = item.replace('-', ' ')
                q[i] = item
            return q
        print('PATENTS QUERY', util(q))

        params = {
            "similarMatch":False,
            "patentTypes":["UTILITY","DESIGN","PLANT"],
            "patentApplicationTypes":["UTILITY","DESIGN","PLANT"],
            "classificationIds":[],
            "listingStatus":[],
            "listingTypes":[],
            "onlyBundles":False,
            "reportId":"db9e8f71-597c-4b37-a908-93d784342afb",
            "primaryKeywords":util(q),
            "secondaryKeywords":[],
            "mustNotKeywords":[],
            "type":"TECHNOLOGIES",
            "cbType":"",
            "sortBy":"revenue",
            "exactMatch":False,
            "sorted":False
        }
        response = requests.post("".join([self.cypris_url, '?resultSize=', str(self.patent_limit), '&offSet=0']), json=params)
        return response.json()['patents']
        
    def get_patents(self):
        """ Pull patents from cypris dev and turn them into pandas dataframe
        """

        # Parse patent results
        patent_df = pd.json_normalize(self._patent_query(self.query))

        # Remove all columns in the set difference
        patent_df = patent_df.drop(list(set(patent_df.columns) - set(self.patent_field_list)) + list(set(self.patent_field_list) - set(patent_df.columns)), axis=1)

        # Rename columns to make more readible
        patent_df = patent_df.rename(columns={
            'country': 'Country Name',
            'patentNumber': 'Publication',
            'publicationDate': 'Publication Date',
            'assignee': 'Applicant',
            'abstraction': 'Abstract',
            'title': 'Title',
            'inventor': 'Inventor',
            'documentType': 'Is granted',
            'categoryId': 'Category',
            'classificationText': 'Classification'
        })
        
        # Generate links
        link_row = []
        link = 'https://worldwide.espacenet.com/patent/search/publication/'
        for idx, row in patent_df.iterrows():
            link_row.append(f'{link}{row.Publication}')
        patent_df['url'] = link_row
        
        # Clean up data
        patent_df['Country Name'] = patent_df['Country Name'].map(lambda x: (pycountry.countries.get(alpha_2=x).name if pycountry.countries.get(alpha_2=x) != None else ''))
        patent_df['Inventor'] = patent_df['Inventor'].map(lambda x: '\n'.join(x))
        patent_df['Applicant'] = patent_df['Applicant'].map(lambda x: '\n'.join(x))
        patent_df['Is granted'] = patent_df['Is granted'].map(lambda x: 'YES' if not x else 'NO')
        patent_df['Category'] = patent_df['Category'].map(lambda x: '\n'.join(x.split('; ')))
        patent_df['Classification'] = patent_df['Classification'].map(lambda x: '\n'.join(x.split('; ')))
        
        # Find secondary keywords
        secondary_array = []
        for index, row in patent_df.iterrows():
            temp = []
            for word in self.secondary_keywords:
                title = '' if not row['Title'] else row['Title'].lower()
                abstract = '' if not row['Abstract'] else row['Abstract'].lower()
                if word.lower() in title or word.lower() in abstract:
                    temp.append(word.lower())
            secondary_array.append(temp)
        patent_df['secondary'] = secondary_array

        return patent_df

    ''' GOOGLE METHODS '''

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

    def to_google(self, news_df, research_df, patent_df):
        """Move data into google sheets and organize in folders correctly.
        """
        scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']

        # Authorize all relevant credentials
        creds = service_account.Credentials.from_service_account_file('google-credentials.json', scopes=scope)
        service = build('drive', 'v3', credentials=creds)
        spreadsheet_service = build('sheets', 'v4', credentials=creds)
        pyg = pygsheets.authorize(custom_credentials=creds)

        folder_id = self._create_folder(service)

        # Create three spreadsheets. One for research, news, and patents
        links = []
        for item in [("research", research_df), ("news", news_df), ('patents', patent_df)]:
            spreadsheet = self._create_spreadsheet(item[0], spreadsheet_service)
            spread_id = spreadsheet.get('spreadsheetId')

            links.append(spread_id)

            worksheet = pyg.open_by_key(spread_id)[0]
            worksheet.set_dataframe(item[1], (0,0))

            self._move_file(folder_id, spread_id, service)

        self._share_folder(folder_id, service)

        return links
