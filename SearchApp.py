from support_functions import Support


class SearchApp:
    
    def __init__(self):
        
        self.support = Support()
        
    def final_search_app(self, category, value, tweet_cache):
        
        output_lst = []
        
        cached_result = tweet_cache.get_from_cache({"category": category, "value": value})
        if cached_result:
            print("Result found in cache.")
            return cached_result["data"]

        if category == 'User':
            documents, documents_exist = self.support.check_doc("screen_name", value, "twitter-data-user")
            if documents_exist:

                user_name = documents[0]['_source']['name']
                user_screen_name = documents[0]['_source']['screen_name']
                user_profile_url = documents[0]['_source']['url']
                user_description = documents[0]['_source']['description']
                user_verification = documents[0]['_source']['verified']
                user_followers_count = documents[0]['_source']['followers_count']
                user_friends_count = documents[0]['_source']['friends_count']
                user_profile_image_url = documents[0]['_source']['profile_image_url_https']

                tl = self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids'])
                tl_lst = []
                for l in tl:
                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                    if o_exist:
                        tl_lst.append(o[0]['_source'])

                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)
                
                xt = self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids'])
                xr = self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids']) 
                xq = self.support.convert_str_to_lst(documents[0]['_source']['quote_ids'])

                t_lst = xt + xr + xq

                for t in t_lst:
                    output_dict = {}
                    docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-tweet")
                    if not docs_exist:
                        docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-retweet")
                        if not docs_exist:
                            docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-quote")

                    if docs_exist:

                        output_dict['text'] = docs[0]['_source']['text']
                        output_dict['quote_count'] = docs[0]['_source']['quote_count']
                        output_dict['reply_count'] = docs[0]['_source']['reply_count']
                        output_dict['retweet_count'] = docs[0]['_source']['retweet_count']
                        output_dict['favorite_count'] = docs[0]['_source']['favorite_count']
                        output_dict['original_tweet_link'] = docs[0]['_source']['original_tweet_link']
                        output_dict['time'] = self.support.str_to_datetime(docs[0]['_source']['created_at'])

                        retweet_meta_data_lst = []
                        if 'retweets' in docs[0]['_source']:
                            rt = self.support.convert_str_to_lst(docs[0]['_source']['retweets'])
                            for r in rt:
                                retweet_meta_data = {"retweet_text": "", "retweet_user": "", "retweet_time": None}
                                m, m_exist = self.support.check_doc("id_str", str(r), "twitter-data-retweet")
                                if m_exist:
                                    retweet_meta_data["retweet_text"] = m[0]['_source']['text']
                                    retweet_meta_data["retweet_time"] = self.support.str_to_datetime(m[0]['_source']['created_at'])
                                    n, n_exist = self.support.check_doc("id_str", m[0]['_source']['retweet_user_id'], "twitter-data-user")
                                    if n_exist:
                                        retweet_meta_data['retweet_user'] = n[0]['_source']['screen_name']
                                retweet_meta_data_lst.append(retweet_meta_data)

                        quote_meta_data_lst = []
                        if 'quotes' in docs[0]['_source']:
                            qt = self.support.convert_str_to_lst(docs[0]['_source']['quotes'])
                            for q in qt:
                                quote_meta_data = {"quote_text": "", "quote_user": "", "quote_time": None}
                                l, l_exist = self.support.check_doc("id_str", str(q), "twitter-data-quote")
                                if l_exist:
                                    quote_meta_data["quote_text"] = l[0]['_source']['text']
                                    quote_meta_data["quote_time"] = self.support.str_to_datetime(l[0]['_source']['created_at'])
                                    p, p_exist = self.support.check_doc("id_str", l[0]['_source']['quote_user_id'], "twitter-data-user")
                                    if p_exist:
                                        quote_meta_data['retweet_user'] = p[0]['_source']['screen_name']
                                quote_meta_data_lst.append(quote_meta_data)

                        output_dict['quote_metadata'] = quote_meta_data_lst
                        output_dict['retweet_metadata'] = retweet_meta_data_lst
                        output_dict['user_name'] = user_name
                        output_dict['user_screen_name'] = user_screen_name
                        output_dict['user_profile_url'] = user_profile_url
                        output_dict['user_description'] = user_description
                        output_dict['user_verification'] = user_verification
                        output_dict['user_followers_count'] = user_followers_count
                        output_dict['user_friends_count'] = user_friends_count
                        output_dict['user_profile_image_url'] = user_profile_image_url
                        output_dict['ordered_user_tweets'] = ordered_lst_tweets
                        output_lst.append(output_dict)
                
                tweet_cache.add_to_cache({"category": category, "value": value}, output_lst)
                return output_lst

        elif category == 'Tweet Text':
            documents_tweet, documents_exist_tweet = self.support.check_doc("text", value, "twitter-data-tweet")
            documents_retweet, documents_exist_retweet = self.support.check_doc("text", value, "twitter-data-retweet")
            documents_quote, documents_exist_quote = self.support.check_doc("text", value, "twitter-data-quote")

            if documents_exist_tweet or documents_exist_retweet or documents_exist_quote:

                documents_lst = documents_tweet + documents_retweet + documents_quote

                for t in documents_lst:
                    output_dict = {}
                    output_dict['text'] = t['_source']['text']
                    output_dict['quote_count'] = t['_source']['quote_count']
                    output_dict['reply_count'] = t['_source']['reply_count']
                    output_dict['retweet_count'] = t['_source']['retweet_count']
                    output_dict['favorite_count'] = t['_source']['favorite_count']
                    output_dict['original_tweet_link'] = t['_source']['original_tweet_link']
                    output_dict['time'] = self.support.str_to_datetime(t['_source']['created_at'])

                    retweet_meta_data_lst = []
                    if 'retweets' in t['_source']:
                        rt = self.support.convert_str_to_lst(t['_source']['retweets'])
                        for r in rt:
                            retweet_meta_data = {"retweet_text": "", "retweet_user": "", "retweet_time": None}
                            m, m_exist = self.support.check_doc("id_str", str(r), "twitter-data-retweet")
                            if m_exist:
                                retweet_meta_data["retweet_text"] = m[0]['_source']['text']
                                retweet_meta_data["retweet_time"] = self.support.str_to_datetime(m[0]['_source']['created_at'])
                                n, n_exist = self.support.check_doc("id_str", m[0]['_source']['retweet_user_id'], "twitter-data-user")
                                if n_exist:
                                    retweet_meta_data['retweet_user'] = n[0]['_source']['screen_name']
                            retweet_meta_data_lst.append(retweet_meta_data)

                    quote_meta_data_lst = []
                    if 'quotes' in t['_source']:
                        qt = self.support.convert_str_to_lst(t['_source']['quotes'])
                        for q in qt:
                            quote_meta_data = {"quote_text": "", "quote_user": "", "quote_time": None}
                            l, l_exist = self.support.check_doc("id_str", str(q), "twitter-data-quote")
                            if l_exist:
                                quote_meta_data["quote_text"] = l[0]['_source']['text']
                                quote_meta_data["quote_time"] = self.support.str_to_datetime(l[0]['_source']['created_at'])
                                p, p_exist = self.support.check_doc("id_str", l[0]['_source']['quote_user_id'], "twitter-data-user")
                                if p_exist:
                                    quote_meta_data['retweet_user'] = p[0]['_source']['screen_name']
                            quote_meta_data_lst.append(quote_meta_data)

                    output_dict['quote_metadata'] = quote_meta_data_lst
                    output_dict['retweet_metadata'] = retweet_meta_data_lst

                    if 'tweet_user_id' in t['_source']:
                        output_dict['user_profile_url'] = t['_source']['tweet_user_profile']
                        doc, doc_exist = self.support.check_doc("id_str", t['_source']['tweet_user_id'], "twitter-data-user")
                        if doc_exist:
                            output_dict['user_name'] = doc[0]['_source']['name']
                            output_dict['user_screen_name'] = doc[0]['_source']['screen_name']
                            output_dict['user_description'] = doc[0]['_source']['description']
                            output_dict['user_verification'] = doc[0]['_source']['verified']
                            output_dict['user_followers_count'] = doc[0]['_source']['followers_count']
                            output_dict['user_friends_count'] = doc[0]['_source']['friends_count']
                            output_dict['user_profile_image_url'] = doc[0]['_source']['profile_image_url_https']

                            tl = self.support.convert_str_to_lst(doc[0]['_source']['tweet_ids'])
                            tl_lst = []
                            for l in tl:
                                o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                if o_exist:
                                    tl_lst.append(o[0]['_source'])

                            ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                            output_dict['ordered_user_tweets'] = ordered_lst_tweets

                    elif 'retweet_user_id' in t['_source']:
                        output_dict['user_profile_url'] = t['_source']['retweet_user_profile']
                        doc, doc_exist = self.support.check_doc("id_str", t['_source']['retweet_user_id'], "twitter-data-user")
                        if doc_exist:
                            output_dict['user_name'] = doc[0]['_source']['name']
                            output_dict['user_screen_name'] = doc[0]['_source']['screen_name']
                            output_dict['user_description'] = doc[0]['_source']['description']
                            output_dict['user_verification'] = doc[0]['_source']['verified']
                            output_dict['user_followers_count'] = doc[0]['_source']['followers_count']
                            output_dict['user_friends_count'] = doc[0]['_source']['friends_count']
                            output_dict['user_profile_image_url'] = doc[0]['_source']['profile_image_url_https']

                            tl = self.support.convert_str_to_lst(doc[0]['_source']['tweet_ids'])
                            tl_lst = []
                            for l in tl:
                                o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                if o_exist:
                                    tl_lst.append(o[0]['_source'])

                            ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                            output_dict['ordered_user_tweets'] = ordered_lst_tweets

                    elif 'quote_user_id' in t['_source']:
                        output_dict['user_profile_url'] = t['_source']['quote_user_profile']
                        doc, doc_exist = self.support.check_doc("id_str", t['_source']['quote_user_id'], "twitter-data-user")
                        if doc_exist:
                            output_dict['user_name'] = doc[0]['_source']['name']
                            output_dict['user_screen_name'] = doc[0]['_source']['screen_name']
                            output_dict['user_description'] = doc[0]['_source']['description']
                            output_dict['user_verification'] = doc[0]['_source']['verified']
                            output_dict['user_followers_count'] = doc[0]['_source']['followers_count']
                            output_dict['user_friends_count'] = doc[0]['_source']['friends_count']
                            output_dict['user_profile_image_url'] = doc[0]['_source']['profile_image_url_https']

                            tl = self.support.convert_str_to_lst(doc[0]['_source']['tweet_ids'])
                            tl_lst = []
                            for l in tl:
                                o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                if o_exist:
                                    tl_lst.append(o[0]['_source'])

                            ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                            output_dict['ordered_user_tweets'] = ordered_lst_tweets
                    else:
                        pass

                    output_lst.append(output_dict)
            
            tweet_cache.add_to_cache({"category": category, "value": value}, output_lst)
            return output_lst

        elif category == 'Hashtag':
            documents, documents_exist = self.support.check_doc("text", value, "twitter-data-hashtag")
            if documents_exist:
                
                xt = self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids']) 
                xr = self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids']) 
                xq = self.support.convert_str_to_lst(documents[0]['_source']['quote_ids'])
                
                t_lst = xt + xr + xq

                for t in t_lst:
                    output_dict = {}
                    docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-tweet")
                    if not docs_exist:
                        docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-retweet")
                        if not docs_exist:
                            docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-quote")

                    if docs_exist:

                        output_dict['text'] = docs[0]['_source']['text']
                        output_dict['quote_count'] = docs[0]['_source']['quote_count']
                        output_dict['reply_count'] = docs[0]['_source']['reply_count']
                        output_dict['retweet_count'] = docs[0]['_source']['retweet_count']
                        output_dict['favorite_count'] = docs[0]['_source']['favorite_count']
                        output_dict['original_tweet_link'] = docs[0]['_source']['original_tweet_link']
                        output_dict['time'] = self.support.str_to_datetime(docs[0]['_source']['created_at'])

                        retweet_meta_data_lst = []
                        if 'retweets' in docs[0]['_source']:
                            rt = self.support.convert_str_to_lst(docs[0]['_source']['retweets'])
                            for r in rt:
                                retweet_meta_data = {"retweet_text": "", "retweet_user": "", "retweet_time": None}
                                m, m_exist = self.support.check_doc("id_str", str(r), "twitter-data-retweet")
                                if m_exist:
                                    retweet_meta_data["retweet_text"] = m[0]['_source']['text']
                                    retweet_meta_data["retweet_time"] = self.support.str_to_datetime(m[0]['_source']['created_at'])
                                    n, n_exist = self.support.check_doc("id_str", m[0]['_source']['retweet_user_id'], "twitter-data-user")
                                    if n_exist:
                                        retweet_meta_data['retweet_user'] = n[0]['_source']['screen_name']
                                retweet_meta_data_lst.append(retweet_meta_data)

                        quote_meta_data_lst = []
                        if 'quotes' in docs[0]['_source']:
                            qt = self.support.convert_str_to_lst(docs[0]['_source']['quotes'])
                            for q in qt:
                                quote_meta_data = {"quote_text": "", "quote_user": "", "quote_time": None}
                                l, l_exist = self.support.check_doc("id_str", str(q), "twitter-data-quote")
                                if l_exist:
                                    quote_meta_data["quote_text"] = l[0]['_source']['text']
                                    quote_meta_data["quote_time"] = self.support.str_to_datetime(l[0]['_source']['created_at'])
                                    p, p_exist = self.support.check_doc("id_str", l[0]['_source']['quote_user_id'], "twitter-data-user")
                                    if p_exist:
                                        quote_meta_data['retweet_user'] = p[0]['_source']['screen_name']
                                quote_meta_data_lst.append(quote_meta_data)

                        output_dict['quote_metadata'] = quote_meta_data_lst
                        output_dict['retweet_metadata'] = retweet_meta_data_lst

                        if 'tweet_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['tweet_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['tweet_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        elif 'retweet_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['retweet_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['retweet_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        elif 'quote_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['quote_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['quote_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        else:
                            pass

                output_lst.append(output_dict)
            
            tweet_cache.add_to_cache({"category": category, "value": value}, output_lst)
            return output_lst

        elif category == 'User Mention':

            documents, documents_exist = self.support.check_doc("screen_name", value, "twitter-data-mention")
            if documents_exist:
                
                xt = self.support.convert_str_to_lst(documents[0]['_source']['tweet_ids'])
                xr = self.support.convert_str_to_lst(documents[0]['_source']['retweet_ids'])
                xq = self.support.convert_str_to_lst(documents[0]['_source']['quote_ids'])

                t_lst = xt + xr + xq

                for t in t_lst:
                    output_dict = {}
                    docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-tweet")
                    if not docs_exist:
                        docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-retweet")
                        if not docs_exist:
                            docs, docs_exist = self.support.check_doc("id_str", str(t), "twitter-data-quote")

                    if docs_exist:

                        output_dict['text'] = docs[0]['_source']['text']
                        output_dict['quote_count'] = docs[0]['_source']['quote_count']
                        output_dict['reply_count'] = docs[0]['_source']['reply_count']
                        output_dict['retweet_count'] = docs[0]['_source']['retweet_count']
                        output_dict['favorite_count'] = docs[0]['_source']['favorite_count']
                        output_dict['original_tweet_link'] = docs[0]['_source']['original_tweet_link']
                        output_dict['time'] = self.support.str_to_datetime(docs[0]['_source']['created_at'])

                        retweet_meta_data_lst = []
                        if 'retweets' in docs[0]['_source']:
                            rt = self.support.convert_str_to_lst(docs[0]['_source']['retweets'])
                            for r in rt:
                                retweet_meta_data = {"retweet_text": "", "retweet_user": "", "retweet_time": None}
                                m, m_exist = self.support.check_doc("id_str", str(r), "twitter-data-retweet")
                                if m_exist:
                                    retweet_meta_data["retweet_text"] = m[0]['_source']['text']
                                    retweet_meta_data["retweet_time"] = self.support.str_to_datetime(m[0]['_source']['created_at'])
                                    n, n_exist = self.support.check_doc("id_str", m[0]['_source']['retweet_user_id'], "twitter-data-user")
                                    if n_exist:
                                        retweet_meta_data['retweet_user'] = n[0]['_source']['screen_name']
                                retweet_meta_data_lst.append(retweet_meta_data)

                        quote_meta_data_lst = []
                        if 'quotes' in docs[0]['_source']:
                            qt = self.support.convert_str_to_lst(docs[0]['_source']['quotes'])
                            for q in qt:
                                quote_meta_data = {"quote_text": "", "quote_user": "", "quote_time": None}
                                l, l_exist = self.support.check_doc("id_str", str(q), "twitter-data-quote")
                                if l_exist:
                                    quote_meta_data["quote_text"] = l[0]['_source']['text']
                                    quote_meta_data["quote_time"] = self.support.str_to_datetime(l[0]['_source']['created_at'])
                                    p, p_exist = self.support.check_doc("id_str", l[0]['_source']['quote_user_id'], "twitter-data-user")
                                    if p_exist:
                                        quote_meta_data['retweet_user'] = p[0]['_source']['screen_name']
                                quote_meta_data_lst.append(quote_meta_data)

                        output_dict['quote_metadata'] = quote_meta_data_lst
                        output_dict['retweet_metadata'] = retweet_meta_data_lst

                        if 'tweet_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['tweet_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['tweet_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        elif 'retweet_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['retweet_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['retweet_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        elif 'quote_user_id' in docs[0]['_source']:
                            output_dict['user_profile_url'] = docs[0]['_source']['quote_user_profile']
                            d, d_exist = self.support.check_doc("id_str", docs[0]['_source']['quote_user_id'], "twitter-data-user")
                            if d_exist:
                                output_dict['user_name'] = d[0]['_source']['name']
                                output_dict['user_screen_name'] = d[0]['_source']['screen_name']
                                output_dict['user_description'] = d[0]['_source']['description']
                                output_dict['user_verification'] = d[0]['_source']['verified']
                                output_dict['user_followers_count'] = d[0]['_source']['followers_count']
                                output_dict['user_friends_count'] = d[0]['_source']['friends_count']
                                output_dict['user_profile_image_url'] = d[0]['_source']['profile_image_url_https']

                                tl = self.support.convert_str_to_lst(d[0]['_source']['tweet_ids'])
                                tl_lst = []
                                for l in tl:
                                    o, o_exist = self.support.check_doc("id_str", str(l), "twitter-data-tweet")
                                    if o_exist:
                                        tl_lst.append(o[0]['_source'])

                                ordered_lst_tweets = self.support.order_by_datetime(tl_lst)

                                output_dict['ordered_user_tweets'] = ordered_lst_tweets

                        else:
                            pass

                output_lst.append(output_dict)
            
            tweet_cache.add_to_cache({"category": category, "value": value}, output_lst)
            return output_lst

        else:
            return output_lst