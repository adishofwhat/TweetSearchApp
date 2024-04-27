from datetime import datetime
from connection import Connection

class Support:
    
    def __init__(self):
        
        self.connection = Connection()
    
    def convert_lst_to_str(self, original_list):
        
        list_as_string = ','.join(map(str, original_list))
        
        return list_as_string
    
    def convert_str_to_lst(self, list_as_string):
        
        if list_as_string:
            string_as_list = list(map(int, list_as_string.split(',')))
        else:
            string_as_list = []
            
        return string_as_list
    
    def check_doc(self, field_to_check, value_to_check, index):
    
        os = self.connection.os_connection()

        match_query = {
        "query": {
            "match": {
                field_to_check: value_to_check
            }}}

        response = None
        while not response:
            try:
                response = os.search(index=index, body=match_query)
            except:
                os = self.connection.os_connection()
                continue

        documents_exist = response["hits"]["total"]["value"] > 0
        documents = response["hits"]["hits"]

        return documents, documents_exist
    
    def str_to_datetime(self,date_string):

        date_format = '%a %b %d %H:%M:%S %z %Y'
        date_time_obj = datetime.strptime(date_string, date_format)
        date_time_only_date = datetime(year=date_time_obj.year, month=date_time_obj.month, day=date_time_obj.day)

        return date_time_only_date
    
    def extract_fields(self, tweet):
        
        tweet['created_at'] = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')

        return {'created_at': tweet['created_at'], 'text': tweet['text']}
    
    def order_by_datetime(self, tweets_data):
        
        if tweets_data == []:
            return []

        else:
            sorted_tweets = sorted(tweets_data, key=lambda x: x['created_at'], reverse=True)
            extracted_fields = [self.extract_fields(tweet) for tweet in sorted_tweets]

        return extracted_fields
    
    def delete_all(self, index_name):
    
        os = self.connection.os_connection()
        
        response = None
        
        while not response:
            delete_query = {
                "query": {
                    "match_all": {}
                }
            }
            try:
                response = os.delete_by_query(index = index_name, body=delete_query)
            except:
                os = self.connection.os_connection()
                continue
                
        return
    
    