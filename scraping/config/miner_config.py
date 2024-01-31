import os
import random

class MinerConfig():

    def __init__(self):
        self.pod_name = os.environ.get("HOSTNAME")
        print("pod_name: ", self.pod_name)

    def get_miner_labels(self, storage):
        miner_labels = storage.check_labels(self.pod_name)
        return miner_labels

    def store_miner_label(self, storage, label):
        miner_data = storage.store_miner_label({"miner_id": self.pod_name, "miner_label": label })
        return miner_data

    def get_random_label(self, configs, miner_labels):

        miner_labels_set = {label["miner_label"] for label in miner_labels} 

        scraper_config = configs.scraper_configs.get('Reddit.custom', None)
        if scraper_config:
            scraper_config.labels_to_scrape[0].label_choices = miner_labels_set

        scraper_id = list(configs.scraper_configs.keys())[0]
        label_config = configs.scraper_configs[scraper_id].labels_to_scrape[0]
        labels = [label.value for label in label_config.label_choices]
        
        random_label = random.choice(labels) if labels else "all"

        return random_label