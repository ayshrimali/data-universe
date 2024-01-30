from scraping.config import model
from scraping import coordinator

class ConfigReader:
    """A class to read the scraping config from a json file."""
    
    @classmethod
    def load_config(cls, filepath: str) -> coordinator.CoordinatorConfig:
        """Loads the scraping config from json and returns it as a CoordinatorConfig.
        
        Raises:
            ValidationError: if the file content is not valid.
        """
        
        print(f"Loading file: {filepath}")
        parsed_file = model.ScrapingConfig.parse_file(path=filepath)
        print(f"Got parsed file: {parsed_file}")
        return parsed_file.to_coordinator_config()


        # """
        # Filter scraping_config file to assign unique labels to miner
        # """ 
        # miner_labels_set = set(label["miner_label"] for label in miner_labels)
        # configs = parsed_file.to_coordinator_config()
        # print("miner_labels_set: ", miner_labels_set)
        # print("configs_before: ", configs)
        

        # for label_to_scrape in configs.scraper_configs['Reddit.custom'].labels_to_scrape:
        #     filtered_labels_to_scrape = []
        #     for data_label in label_to_scrape.label_choices:
        #         if data_label.value in miner_labels_set:
        #             filtered_labels_to_scrape.append(data_label)
        #     configs.scraper_configs['Reddit.custom'].labels_to_scrape[0].label_choices = filtered_labels_to_scrape

        # print("configs_after: ", configs)

        # return configs
    