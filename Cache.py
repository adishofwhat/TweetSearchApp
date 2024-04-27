import threading
import time
import re
import json
from collections import OrderedDict

class TweetCache:
    
    def __init__(self, max_size=30):
        self.cache = OrderedDict()  
        self.id_list = []  
        self.max_size = max_size  
        self.refresh_time = 30 * 60
        self.last_refresh_time = time.time()

        self.refresh_thread = threading.Thread(target=self._auto_refresh_cache)
        self.refresh_thread.daemon = True
        self.refresh_thread.start()

    def _refresh_cache(self):
        print("Refreshing cache...")
        current_time = time.time()
        expired_items = []
        for query_id, data in self.cache.items():
            if current_time - data["timestamp"] >= self.refresh_time:
                expired_items.append(query_id)
        for query_id in expired_items:
            del self.cache[query_id]
            self.id_list.remove(query_id)  
        self.last_refresh_time = current_time

    def _normalize_query_input(self, query):
        normalized_values = {key: self._clean_text(str(value).lower()) for key, value in query.items()}
        return normalized_values

    def _clean_text(self, text):
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        return cleaned_text.strip()

    def _get_ui_input_concatenated(self, query):
        return ''.join(self._normalize_query_input(query).values())

    def add_to_cache(self, query, data):
        ui_input_concatenated = self._get_ui_input_concatenated(query)
        if ui_input_concatenated in self.cache:
            del self.cache[ui_input_concatenated]  
            self.id_list.remove(ui_input_concatenated)
        elif len(self.cache) >= self.max_size:  
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]
            self.id_list.remove(oldest_key)
        self.cache[ui_input_concatenated] = {
            'ID': ui_input_concatenated,
            'Query': query,
            'data': data,
            'timestamp': time.time()  
        }
        self.id_list.append(ui_input_concatenated)  
        
    def get_from_cache(self, query):
        ui_input_concatenated = self._get_ui_input_concatenated(query)
        if ui_input_concatenated in self.cache:
            self.cache.move_to_end(ui_input_concatenated)  
            return self.cache[ui_input_concatenated]
        return None

    def display_cache(self):
        print("Current Cache Contents:")
        print("ID List:", self.id_list)
        print("--------------------")
        for query_id, data in self.cache.items():
            print("Query ID:", query_id)
            print("Query:", data["Query"])
            print("Data:", data["data"])
            print("Timestamp:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data["timestamp"])))
            print("--------------------")

    def export_to_json(self, filename):
        cache_data = {"cache": self.cache, "id_list": self.id_list}
        with open(filename, 'w') as f:
            json.dump(cache_data, f)

    def _auto_refresh_cache(self):
        while True:
            current_time = time.time()
            if current_time - self.last_refresh_time >= self.refresh_time:
                self._refresh_cache()
                print("Cache refreshed at:", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time)))
            time.sleep(self.refresh_time / 2)

