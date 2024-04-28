import json
from connection import Connection
from support_functions import Support

class DataCuration:
    
    def __init__(self):
        
        self.connection = Connection()
        self.support = Support()
    
    def load_data(self):
    
        lst_tweet_data = []

        with open("corona-out-2", "r") as f1:
            for line in f1:
                try:
                    data = json.loads(line)
                    lst_tweet_data.append(data)
                except:
                    continue

        return lst_tweet_data

    def stream_data(self):
    
        lst_tweet_data = []

        with open("corona-out-3", "r") as f1:
            for line in f1:
                try:
                    data = json.loads(line)
                    
                    user_lst = self.get_users([data])
                    tweet_lst, retweet_lst, quote_lst = self.get_tweets([data])
                    hashtag_lst = self.get_hashtags([data])
                    user_mention_lst = self.get_user_mentions([data])
                    
                    self.add_users(user_lst)
                    self.add_tweets(tweet_lst)
                    self.add_retweets(retweet_lst)
                    self.add_quotes(quote_lst)
                    self.add_hashtags(hashtag_lst)
                    self.add_user_mentions(user_mention_lst)
                    
                except:
                    continue

        return

## Uncomment the code below to load the 'corona-out-3' file
#     def stream_data(self):
    
#         lst_tweet_data = []

#         with open("corona-out-3", "r") as f1:
#             for line in f1:
#                 try:
#                     data = json.loads(line)
#                     lst_tweet_data.append(data)
#                 except:
#                     continue

#         return lst_tweet_data
    
    def get_users(self, lst_tweet_data):
        
        user_lst = []
        
        for tweet_dict in lst_tweet_data:
            
            keys_to_pop = ["profile_background_color", "profile_background_image_url", "profile_background_image_url_https", 
                           "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", "profile_sidebar_fill_color", 
                           "profile_text_color", "profile_use_background_image", "profile_banner_url", "utc_offset", "time_zone", "geo_enabled", 
                           "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications"]

            if 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' in tweet_dict:   
                r_user = {key: tweet_dict['user'][key] for key in tweet_dict['user'] if key not in keys_to_pop}
                r_user['retweet_ids'], r_user['tweet_ids'], r_user['quote_ids'] = [], [], []
                r_user['retweet_ids'].append(tweet_dict['id_str'])
                user_lst.append(r_user)

                q_user = {key: tweet_dict['retweeted_status']['user'][key] for key in tweet_dict['retweeted_status']['user'] if key not in keys_to_pop}
                q_user['retweet_ids'], q_user['tweet_ids'], q_user['quote_ids'] = [], [], []
                q_user['quote_ids'].append(tweet_dict['retweeted_status']['id_str'])
                user_lst.append(q_user)

                user = {key: tweet_dict['quoted_status']['user'][key] for key in tweet_dict['quoted_status']['user'] if key not in keys_to_pop}
                user['retweet_ids'], user['tweet_ids'], user['quote_ids'] = [], [], []
                user['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])

                user_lst.append(user)

            elif 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' not in tweet_dict:
                r_user = {key: tweet_dict['user'][key] for key in tweet_dict['user'] if key not in keys_to_pop}
                r_user['retweet_ids'], r_user['tweet_ids'], r_user['quote_ids'] = [], [], []
                r_user['retweet_ids'].append(tweet_dict['id_str'])
                user_lst.append(r_user)

                user = {key: tweet_dict['retweeted_status']['user'][key] for key in tweet_dict['retweeted_status']['user'] if key not in keys_to_pop}
                user['retweet_ids'], user['tweet_ids'], user['quote_ids'] = [], [], []
                user['tweet_ids'].append(tweet_dict['retweeted_status']['id_str'])

                user_lst.append(user)

            elif 'user' in tweet_dict and 'quoted_status' in tweet_dict and 'retweeted_status' not in tweet_dict:
                q_user = {key: tweet_dict['user'][key] for key in tweet_dict['user'] if key not in keys_to_pop}
                q_user['retweet_ids'], q_user['tweet_ids'], q_user['quote_ids'] = [], [], []
                q_user['quote_ids'].append(tweet_dict['id_str'])
                user_lst.append(q_user)

                user = {key: tweet_dict['quoted_status']['user'][key] for key in tweet_dict['quoted_status']['user'] if key not in keys_to_pop}
                user['retweet_ids'], user['tweet_ids'], user['quote_ids'] = [], [], []
                user['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])

                user_lst.append(user)

            elif 'user' in tweet_dict and 'retweeted_status' not in tweet_dict and 'quoted_status' not in tweet_dict:
                user = {key: tweet_dict['user'][key] for key in tweet_dict['user'] if key not in keys_to_pop}
                user['retweet_ids'], user['tweet_ids'], user['quote_ids'] = [], [], []
                user['tweet_ids'].append(tweet_dict['id_str'])

                user_lst.append(user)

            elif 'user' not in tweet_dict:
                pass

            else:
                pass

        return user_lst
    
    def get_tweets(self, lst_tweet_data):
        
        tweet_lst, retweet_lst, quote_lst = [], [], []

        for tweet_dict in lst_tweet_data:

            keys_to_pop = ["source", "user", "geo", "coordinates", "retweeted_status", "quoted_status_id", "quoted_status_id_str", "quoted_status", 
                           "quoted_status_permalink", "timestamp_ms", "entities", "contributors", "filter_level"]

            if 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' in tweet_dict:

                retweet_id_str = tweet_dict['id_str']
                quote_id_str = tweet_dict['retweeted_status']['id_str']
                tweet_id_str = tweet_dict['quoted_status']['id_str']

                retweet_user_id_str = tweet_dict['user']['id_str']
                retweet_user_profile_url = tweet_dict['user']['url']

                quote_user_id_str = tweet_dict['retweeted_status']['user']['id_str']
                quote_user_profile_url = tweet_dict['retweeted_status']['user']['url']

                tweet_user_id_str = tweet_dict['quoted_status']['user']['id_str']
                tweet_user_profile_url = tweet_dict['quoted_status']['user']['url']

                original_tweet_link = ""
                if 'quoted_status_permalink' in tweet_dict:
                    if tweet_dict['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status_permalink']['expanded']
                elif 'quoted_status_permalink' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['retweeted_status']['quoted_status_permalink']['expanded']
                elif 'quoted_status_permalink' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status']['quoted_status_permalink']['expanded']
                else:
                    pass

                retweet = {key: tweet_dict[key] for key in tweet_dict if key not in keys_to_pop}
                quote = {key: tweet_dict['retweeted_status'][key] for key in tweet_dict['retweeted_status'] if key not in keys_to_pop}
                tweet = {key: tweet_dict['quoted_status'][key] for key in tweet_dict['quoted_status'] if key not in keys_to_pop}

                if retweet['place']:
                    retweet['place'] = retweet['place']['full_name']
                retweet['quote'] = quote_id_str
                retweet['tweet'] = tweet_id_str
                retweet['original_tweet_link'] = original_tweet_link
                retweet['retweet_user_id'] = retweet_user_id_str
                retweet['retweet_user_profile'] = retweet_user_profile_url

                if quote['place']:
                    quote['place'] = quote['place']['full_name']
                quote['retweet'] = retweet_id_str
                quote['tweet'] = tweet_id_str
                quote['original_tweet_link'] = original_tweet_link
                quote['quote_user_id'] = quote_user_id_str
                quote['quote_user_profile'] = quote_user_profile_url

                if tweet['place']:
                    tweet['place'] = tweet['place']['full_name']
                tweet['quotes'], tweet['retweets'] = [], []
                tweet['quotes'].append(quote_id_str)
                tweet['retweets'].append(retweet_id_str)
                tweet['original_tweet_link'] = original_tweet_link
                tweet['tweet_user_id'] = tweet_user_id_str
                tweet['tweet_user_profile'] = tweet_user_profile_url

                tweet_lst.append(tweet)
                retweet_lst.append(retweet)
                quote_lst.append(quote)

            elif 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' not in tweet_dict:
                retweet_id_str = tweet_dict['id_str']
                tweet_id_str = tweet_dict['retweeted_status']['id_str']

                retweet_user_id_str = tweet_dict['user']['id_str']
                retweet_user_profile_url = tweet_dict['user']['url']

                tweet_user_id_str = tweet_dict['retweeted_status']['user']['id_str']
                tweet_user_profile_url = tweet_dict['retweeted_status']['user']['url']

                original_tweet_link = ""
                if 'quoted_status_permalink' in tweet_dict:
                    if tweet_dict['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status_permalink']['expanded']
                elif 'quoted_status_permalink' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['retweeted_status']['quoted_status_permalink']['expanded']
                else:
                    pass

                retweet = {key: tweet_dict[key] for key in tweet_dict if key not in keys_to_pop}
                tweet = {key: tweet_dict['retweeted_status'][key] for key in tweet_dict['retweeted_status'] if key not in keys_to_pop}

                if retweet['place']:
                    retweet['place'] = retweet['place']['full_name']
                retweet['tweet'] = tweet_id_str
                retweet['original_tweet_link'] = original_tweet_link
                retweet['retweet_user_id'] = retweet_user_id_str
                retweet['retweet_user_profile'] = retweet_user_profile_url

                if tweet['place']:
                    tweet['place'] = tweet['place']['full_name']
                tweet['quotes'], tweet['retweets'] = [], []
                tweet['retweets'].append(retweet_id_str)
                tweet['original_tweet_link'] = original_tweet_link
                tweet['tweet_user_id'] = tweet_user_id_str
                tweet['tweet_user_profile'] = tweet_user_profile_url

                tweet_lst.append(tweet)
                retweet_lst.append(retweet)

            elif 'user' in tweet_dict and 'quoted_status' in tweet_dict and 'retweeted_status' not in tweet_dict:
                quote_id_str = tweet_dict['id_str']
                tweet_id_str = tweet_dict['quoted_status']['id_str']

                quote_user_id_str = tweet_dict['user']['id_str']
                quote_user_profile_url = tweet_dict['user']['url']

                tweet_user_id_str = tweet_dict['quoted_status']['user']['id_str']
                tweet_user_profile_url = tweet_dict['quoted_status']['user']['url']

                original_tweet_link = ""
                if 'quoted_status_permalink' in tweet_dict:
                    if tweet_dict['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status_permalink']['expanded']
                elif 'quoted_status_permalink' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status']['quoted_status_permalink']['expanded']
                else:
                    pass

                quote = {key: tweet_dict[key] for key in tweet_dict if key not in keys_to_pop}
                tweet = {key: tweet_dict['quoted_status'][key] for key in tweet_dict['quoted_status'] if key not in keys_to_pop}

                if quote['place']:
                    quote['place'] = quote['place']['full_name']
                quote['tweet'] = tweet_id_str
                quote['original_tweet_link'] = original_tweet_link
                quote['quote_user_id'] = quote_user_id_str
                quote['quote_user_profile'] = quote_user_profile_url

                if tweet['place']:
                    tweet['place'] = tweet['place']['full_name']
                tweet['quotes'], tweet['retweets'] = [], []
                tweet['quotes'].append(quote_id_str)
                tweet['original_tweet_link'] = original_tweet_link
                tweet['tweet_user_id'] = tweet_user_id_str
                tweet['tweet_user_profile'] = tweet_user_profile_url

                tweet_lst.append(tweet)
                quote_lst.append(quote)

            elif 'user' in tweet_dict and 'quoted_status' not in tweet_dict and 'retweeted_status' not in tweet_dict:
                tweet_id_str = tweet_dict['id_str']

                tweet_user_id_str = tweet_dict['user']['id_str']
                tweet_user_profile_url = tweet_dict['user']['url']

                original_tweet_link = ""
                if 'quoted_status_permalink' in tweet_dict:
                    if tweet_dict['quoted_status_permalink']:
                        original_tweet_link = tweet_dict['quoted_status_permalink']['expanded']
                else:
                    pass

                tweet = {key: tweet_dict[key] for key in tweet_dict if key not in keys_to_pop}

                if tweet['place']:
                    tweet['place'] = tweet['place']['full_name']
                tweet['quotes'], tweet['retweets'] = [], []
                tweet['original_tweet_link'] = original_tweet_link
                tweet['tweet_user_id'] = tweet_user_id_str
                tweet['tweet_user_profile'] = tweet_user_profile_url

            elif 'user' not in tweet_dict:
                pass

            else:
                pass

        return tweet_lst, retweet_lst, quote_lst
    
    def get_hashtags(self, lst_tweet_data):
        
        hashtag_lst = []

        for tweet_dict in lst_tweet_data:

            if 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' in tweet_dict:
                if tweet_dict['entities']['hashtags']:
                    for ht in tweet_dict['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['retweet_ids'].append(tweet_dict['id_str'])
                        hashtag_lst.append(hashtag)
                elif tweet_dict['retweeted_status']['entities']['hashtags']:
                    for ht in tweet_dict['retweeted_status']['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['quote_ids'].append(tweet_dict['retweeted_status']['id_str'])
                        hashtag_lst.append(hashtag)
                elif tweet_dict['quoted_status']['entities']['hashtags']:
                    for ht in tweet_dict['quoted_status']['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                        hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['retweet_ids'].append(tweet_dict['id_str'])
                            hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['retweeted_status']['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['quote_ids'].append(tweet_dict['retweeted_status']['id_str'])
                            hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['quoted_status']['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                            hashtag_lst.append(hashtag)
                else:
                    pass

            elif 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' not in tweet_dict:
                if tweet_dict['entities']['hashtags']:
                    for ht in tweet_dict['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['retweet_ids'].append(tweet_dict['id_str'])
                        hashtag_lst.append(hashtag)
                elif tweet_dict['retweeted_status']['entities']['hashtags']:
                    for ht in tweet_dict['retweeted_status']['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['tweet_ids'].append(tweet_dict['retweeted_status']['id_str'])
                        hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['retweet_ids'].append(tweet_dict['id_str'])
                            hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['retweeted_status']['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['tweet_ids'].append(tweet_dict['retweeted_status']['id_str'])
                            hashtag_lst.append(hashtag)
                else:
                    pass

            elif 'user' in tweet_dict and 'quoted_status' in tweet_dict and 'retweeted_status' not in tweet_dict:
                if tweet_dict['entities']['hashtags']:
                    for ht in tweet_dict['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['quote_ids'].append(tweet_dict['id_str'])
                        hashtag_lst.append(hashtag)
                elif tweet_dict['quoted_status']['entities']['hashtags']:
                    for ht in tweet_dict['quoted_status']['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                        hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['quote_ids'].append(tweet_dict['id_str'])
                            hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['quoted_status']['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                            hashtag_lst.append(hashtag)
                else:
                    pass

            elif 'user' in tweet_dict and 'quoted_status' not in tweet_dict and 'retweeted_status' not in tweet_dict:
                if tweet_dict['entities']['hashtags']:
                    for ht in tweet_dict['entities']['hashtags']:
                        hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        hashtag['text'] = ht['text']
                        hashtag['tweet_ids'].append(tweet_dict['id_str'])
                        hashtag_lst.append(hashtag)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['hashtags']:
                        for ht in tweet_dict['extended_tweet']['entities']['hashtags']:
                            hashtag = {"text": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            hashtag['text'] = ht['text']
                            hashtag['tweet_ids'].append(tweet_dict['id_str'])
                            hashtag_lst.append(hashtag)   
            else:
                pass    

        return hashtag_lst
    
    def get_user_mentions(self, lst_tweet_data):
        
        user_mention_lst = []

        for tweet_dict in lst_tweet_data:

            if 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' in tweet_dict:
                if tweet_dict['entities']['user_mentions']:
                    for um in tweet_dict['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['retweet_ids'].append(tweet_dict['id_str'])
                        user_mention_lst.append(user_mention)
                elif tweet_dict['retweeted_status']['entities']['user_mentions']:
                    for um in tweet_dict['retweeted_status']['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['quote_ids'].append(tweet_dict['retweeted_status']['id_str'])
                        user_mention_lst.append(user_mention)
                elif tweet_dict['quoted_status']['entities']['user_mentions']:
                    for um in tweet_dict['quoted_status']['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                        user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['retweet_ids'].append(tweet_dict['id_str'])
                            user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['retweeted_status']['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['quote_ids'].append(tweet_dict['retweeted_status']['id_str'])
                            user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['quoted_status']['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                            user_mention_lst.append(user_mention)
                else:
                    pass

            elif 'user' in tweet_dict and 'retweeted_status' in tweet_dict and 'quoted_status' not in tweet_dict:
                if tweet_dict['entities']['user_mentions']:
                    for um in tweet_dict['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['retweet_ids'].append(tweet_dict['id_str'])
                        user_mention_lst.append(user_mention)
                elif tweet_dict['retweeted_status']['entities']['user_mentions']:
                    for um in tweet_dict['retweeted_status']['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['tweet_ids'].append(tweet_dict['retweeted_status']['id_str'])
                        user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['retweet_ids'].append(tweet_dict['id_str'])
                            user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict['retweeted_status']:
                    if tweet_dict['retweeted_status']['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['retweeted_status']['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['tweet_ids'].append(tweet_dict['retweeted_status']['id_str'])
                            user_mention_lst.append(user_mention)
                else:
                    pass

            elif 'user' in tweet_dict and 'quoted_status' in tweet_dict and 'retweeted_status' not in tweet_dict:
                if tweet_dict['entities']['user_mentions']:
                    for um in tweet_dict['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['quote_ids'].append(tweet_dict['id_str'])
                        user_mention_lst.append(user_mention)
                elif tweet_dict['quoted_status']['entities']['user_mentions']:
                    for um in tweet_dict['quoted_status']['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                        user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict:
                    if tweet_dict['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['quote_ids'].append(tweet_dict['id_str'])
                            user_mention_lst.append(user_mention)
                elif 'extended_tweet' in tweet_dict['quoted_status']:
                    if tweet_dict['quoted_status']['extended_tweet']['entities']['user_mentions']:
                        for um in tweet_dict['quoted_status']['extended_tweet']['entities']['user_mentions']:
                            user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                            user_mention['screen_name'] = um['screen_name']
                            user_mention['name'] = um['name']
                            user_mention['id_str'] = um['id_str']
                            user_mention['tweet_ids'].append(tweet_dict['quoted_status']['id_str'])
                            user_mention_lst.append(user_mention)
                else:
                    pass

            elif 'user' in tweet_dict and 'quoted_status' not in tweet_dict and 'retweeted_status' not in tweet_dict:
                if tweet_dict['entities']['user_mentions']:
                    for um in tweet_dict['entities']['user_mentions']:
                        user_mention = {"screen_name": "", "name": "", "id_str": "", "tweet_ids": [], "retweet_ids": [], "quote_ids": []}
                        user_mention['screen_name'] = um['screen_name']
                        user_mention['name'] = um['name']
                        user_mention['id_str'] = um['id_str']
                        user_mention['tweet_ids'].append(tweet_dict['id_str'])
                        user_mention_lst.append(user_mention)
            else:
                pass

        return user_mention_lst
    
    def add_users(self, user_lst):
        
        os = self.connection.os_connection()

        for user in user_lst:
            documents, documents_exist = self.support.check_doc("id_str", user['id_str'], "twitter-data-user")
            if documents_exist:
                new_tweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids']) + user['tweet_ids'])
                new_retweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids']) + 
                                                                  user['retweet_ids'])
                new_quote_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['quote_ids']) + user['quote_ids'])

                update_query = {
                    "script": {
                        "source": """
                            ctx._source.tweet_ids = params.new_value1;
                            ctx._source.retweet_ids = params.new_value2;
                            ctx._source.quote_ids = params.new_value3;
                        """,
                        "params": {
                            "new_value1": new_tweet_ids,
                            "new_value2": new_retweet_ids,
                            "new_value3": new_quote_ids
                        }
                    },
                    "query": {
                        "match": {
                            "id_str": documents[0]['_source']['id_str']
                        }
                    }
                }

                response = None
                while not response:
                    try:
                        response = os.update_by_query(index = "twitter-data-user", body=update_query)
                    except:
                        os = self.connection.os_connection()
                        continue

            else:
                user['tweet_ids'] = self.support.convert_lst_to_str(user['tweet_ids'])
                user['retweet_ids'] = self.support.convert_lst_to_str(user['retweet_ids'])
                user['quote_ids'] = self.support.convert_lst_to_str(user['quote_ids'])

                response = None
                while not response:
                    try:
                        response = os.index(index = "twitter-data-user",body = user)
                    except:
                        os = self.connection.os_connection()
                        continue

        return
    
    def add_tweets(self, tweet_lst):
        
        os = self.connection.os_connection()

        for tweet in tweet_lst:
            documents, documents_exist = self.support.check_doc("id_str", tweet['id_str'], "twitter-data-tweet")
            if documents_exist:
                new_retweets = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['retweets']) + tweet['retweets'])
                new_quotes = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['quotes']) + tweet['quotes'])

                update_query = {
                    "script": {
                        "source": """
                            ctx._source.retweets = params.new_value1;
                            ctx._source.quotes = params.new_value2;
                        """,
                        "params": {
                            "new_value1": new_retweets,
                            "new_value2": new_quotes
                        }
                    },
                    "query": {
                        "match": {
                            "id_str": documents[0]['_source']['id_str']
                        }
                    }
                }

                response = None
                while not response:
                    try:
                        response = os.update_by_query(index = "twitter-data-tweet", body=update_query)
                    except:
                        os = self.connection.os_connection()
                        continue

            else:
                tweet['retweets'] = self.support.convert_lst_to_str(tweet['retweets'])
                tweet['quotes'] = self.support.convert_lst_to_str(tweet['quotes'])

                response = None
                while not response:
                    try:
                        response = os.index(index = "twitter-data-tweet",body = tweet)
                    except:
                        os = self.connection.os_connection()
                        continue

        return
    
    def add_retweets(self, retweet_lst):
        
        search = self.connection.os_connection()

        for retweet in retweet_lst:
            try:
                search.index(index = "twitter-data-retweet",body = retweet)
            except:
                search = self.connection.os_connection()
                continue

        return
    
    def add_quotes(self, quote_lst):
        
        search = self.connection.os_connection()

        for quote in quote_lst:
            try:
                search.index(index = "twitter-data-quote",body = quote)
            except:
                search = self.connection.os_connection()
                continue

        return
    
    def add_hashtags(self, hashtag_lst):
        
        os = self.connection.os_connection()

        for hashtag in hashtag_lst:
            documents, documents_exist = self.support.check_doc("text", hashtag['text'], "twitter-data-hashtag")
            if documents_exist:
                new_tweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids']) + hashtag['tweet_ids'])
                new_retweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids']) + 
                                                                  hashtag['retweet_ids'])
                new_quote_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['quote_ids']) + hashtag['quote_ids'])

                update_query = {
                    "script": {
                        "source": """
                            ctx._source.tweet_ids = params.new_value1;
                            ctx._source.retweet_ids = params.new_value2;
                            ctx._source.quote_ids = params.new_value3;
                        """,
                        "params": {
                            "new_value1": new_tweet_ids,
                            "new_value2": new_retweet_ids,
                            "new_value3": new_quote_ids
                        }
                    },
                    "query": {
                        "match": {
                            "id_str": documents[0]['_source']['text']
                        }
                    }
                }

                response = None
                while not response:
                    try:
                        response = os.update_by_query(index = "twitter-data-hashtag", body=update_query)
                    except:
                        os = self.connection.os_connection()
                        continue

            else:
                hashtag['tweet_ids'] = self.support.convert_lst_to_str(hashtag['tweet_ids'])
                hashtag['retweet_ids'] = self.support.convert_lst_to_str(hashtag['retweet_ids'])
                hashtag['quote_ids'] = self.support.convert_lst_to_str(hashtag['quote_ids'])

                response = None
                while not response:
                    try:
                        response = os.index(index = "twitter-data-hashtag",body = hashtag)
                    except:
                        os = self.connection.os_connection()
                        continue

        return
    
    def add_user_mentions(self, user_mention_lst):
        
        os = self.connection.os_connection()

        for user_mention in user_mention_lst:
            documents, documents_exist = self.support.check_doc("screen_name", user_mention['screen_name'], "twitter-data-mention")
            if documents_exist:
                new_tweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids']) + 
                                                                user_mention['tweet_ids'])
                new_retweet_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids']) + 
                                                                  user_mention['retweet_ids'])
                new_quote_ids = self.support.convert_lst_to_str(self.support.convert_str_to_lst(documents[0]['_source']['quote_ids']) + 
                                                                user_mention['quote_ids'])

                update_query = {
                    "script": {
                        "source": """
                            ctx._source.tweet_ids = params.new_value1;
                            ctx._source.retweet_ids = params.new_value2;
                            ctx._source.quote_ids = params.new_value3;
                        """,
                        "params": {
                            "new_value1": new_tweet_ids,
                            "new_value2": new_retweet_ids,
                            "new_value3": new_quote_ids
                        }
                    },
                    "query": {
                        "match": {
                            "id_str": documents[0]['_source']['screen_name']
                        }
                    }
                }

                response = None
                while not response:
                    try:
                        response = os.update_by_query(index = "twitter-data-mention", body=update_query)
                    except:
                        os = self.connection.os_connection()
                        continue

            else:
                user_mention['tweet_ids'] = self.support.convert_lst_to_str(user_mention['tweet_ids'])
                user_mention['retweet_ids'] = self.support.convert_lst_to_str(user_mention['retweet_ids'])
                user_mention['quote_ids'] = self.support.convert_lst_to_str(user_mention['quote_ids'])

                response = None
                while not response:
                    try:
                        response = os.index(index = "twitter-data-mention",body = user_mention)
                    except:
                        os = self.connection.os_connection()
                        continue

        return

data_curation = DataCuration()

lst_tweet_data = data_curation.stream_data()
user_lst = data_curation.get_users(lst_tweet_data)
tweet_lst, retweet_lst, quote_lst = data_curation.get_tweets(lst_tweet_data)
hashtag_lst = data_curation.get_hashtags(lst_tweet_data)
user_mention_lst = data_curation.get_user_mentions(lst_tweet_data)

data_curation.add_users(user_lst)
data_curaton.add_tweets(tweet_lst)
data_curaton.add_retweets(retweet_lst)
data_curaton.add_quotes(quote_lst)
data_curaton.add_hashtags(hashtag_lst)
data_curaton.add_user_mentions(user_mention_lst)

##Uncomment the code below to treat the 'corona-out-3' file as a stream and load the data one by one into the data streams
#data_curation.stream_data()
