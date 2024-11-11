# pipelines.py
import json
import os

class CompanyProfilePipeline:
    scraped_urls = set()  # Make scraped_urls globally accessible to the spider

    def open_spider(self, spider):
        self.output_file = 'company_profile_data.json'
        
        # Load existing data if available
        if os.path.exists(self.output_file):
            with open(self.output_file, 'r') as f:
                try:
                    existing_data = json.load(f)
                    # Populate `scraped_urls` with URLs that have already been scraped
                    CompanyProfilePipeline.scraped_urls = {item['company_url'] for item in existing_data}
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []
        
        # Track all scraped data
        self.existing_data = existing_data

    def process_item(self, item, spider):
        # Only add new items to avoid duplicates
        if item['company_url'] not in CompanyProfilePipeline.scraped_urls:
            self.existing_data.append(dict(item))
            CompanyProfilePipeline.scraped_urls.add(item['company_url'])
        return item

    def close_spider(self, spider):
        # Save data to the JSON file when the spider closes
        with open(self.output_file, 'w') as f:
            json.dump(self.existing_data, f, indent=4)
