import json
import os

class CompanyProfilePipeline:
    def open_spider(self, spider):
        self.output_file = 'company_profile_data.json'
        
        # Load existing data if available
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r') as f:
                try:
                    self.existing_data = json.load(f)
                except json.JSONDecodeError:
                    self.existing_data = []
        else:
            self.existing_data = []
        
        # Keep track of URLs already scraped
        self.scraped_urls = {item['company_url'] for item in self.existing_data}

    def process_item(self, item, spider):
        # Only add new items to avoid duplicates
        if item['company_url'] not in self.scraped_urls:
            self.existing_data.append(dict(item))
            self.scraped_urls.add(item['company_url'])
        return item

    def close_spider(self, spider):
        # Save data to the JSON file when the spider closes
        with open(self.output_file, 'w') as f:
            json.dump(self.existing_data, f, indent=4)
