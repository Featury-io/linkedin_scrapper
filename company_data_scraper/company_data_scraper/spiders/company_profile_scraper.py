import json
import time
from typing import Any, Iterable
import scrapy
from scrapy.http import Request, Response
import re

input_file = 'company_names.json'
company_urls = []

def get_url_by_company_name():
    global company_urls
    try:
        with open(input_file, 'r') as json_file:
            data = json.load(json_file)
            for v in data.values():
                company_urls.append(v + "/?trk=companies_directory")
            print("Company URLs:", len(company_urls))
    except FileNotFoundError:
        print(f"Error: JSON file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred while reading JSON file: {str(e)}")

class CompanyProfileScraperSpider(scrapy.Spider):
    name = 'company_profile_scraper'
    
    custom_settings = {
        'HTTPERROR_ALLOW_ALL': True,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 5,
        'DOWNLOAD_DELAY': 3,              # Increased download delay to 3 seconds
        'RANDOMIZE_DOWNLOAD_DELAY': True, # Randomize download delay for natural behavior
        'RETRY_TIMES': 10,
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        get_url_by_company_name()
        
        if not company_urls:
            print("No company URLs found. Exiting spider.")
            raise ValueError("No URLs to scrape.")

        self.company_pages = list(set(company_urls.copy()))
        print(f"Found {len(self.company_pages)} URLs to scrape.")

    def start_requests(self):
        for idx, url in enumerate(self.company_pages):
            yield scrapy.Request(
                url=url,
                callback=self.parse_response,
                meta={'company_index_tracker': idx, 'company_url': url, 'retry_count': 0}
            )

    def parse_response(self, response):
        company_index_tracker = response.meta['company_index_tracker']
        company_url = self.company_pages[company_index_tracker]
        retry_count = response.meta.get('retry_count', 0)

        # Handle redirects
        if response.status in [301, 302, 303, 307, 308]:
            print(f"Redirect encountered for {response.url}. Handling redirect.")
            yield scrapy.Request(
                url=response.headers['Location'].decode('utf-8'), 
                callback=self.parse_response,
                meta={'company_index_tracker': company_index_tracker, 'retry_count': retry_count},
                dont_filter=True
            )
            return

        # Retry on 404 errors
        if response.status == 404:
            if retry_count < 3:
                print(f"Page not found for {company_url}. Retrying ({retry_count + 1}/10)...")
                time.sleep(3)  # Additional delay before retrying
                yield scrapy.Request(
                    url=company_url,
                    callback=self.parse_response,
                    meta={'company_index_tracker': company_index_tracker, 'retry_count': retry_count + 1},
                    dont_filter=True
                )
            else:
                print(f"Max retries reached for {company_url}. Skipping.")
            return

        print('********')
        print(f'Scraping page: {str(company_index_tracker + 1)} of {str(len(self.company_pages))}')
        print('********')

        company_item = {}
        company_item['company_url'] = company_url
        company_item['company_name'] = response.css('.top-card-layout__entity-info h1::text').get(default='not-found').strip()

        # Pause for an additional 3 seconds if company_name is 'not-found'
        if company_item['company_name'] == 'not-found':
            print("Company name not found. Pausing for an additional 3 seconds.")
            time.sleep(3)

        followers_count_text = response.xpath('//h3[contains(@class, "top-card-layout__first-subline")]/span/following-sibling::text()').get(default='not-found')
        try:
            company_item['linkedin_followers_count'] = int(followers_count_text.split()[0].strip().replace(',', '')) if followers_count_text != 'not-found' else None
        except (ValueError, AttributeError):
            company_item['linkedin_followers_count'] = None

        company_item['company_logo_url'] = response.css('div.top-card-layout__entity-image-container img::attr(data-delayed-url)').get(default='not-found')
        company_item['about_us'] = response.css('.core-section-container__content p::text').get(default='not-found').strip()

        try:
            followers_num_match = re.findall(r'\d{1,3}(?:,\d{3})*', response.css('a.face-pile__cta::text').get(default='not-found').strip())
            company_item['num_of_employees'] = int(followers_num_match[0].replace(',', '')) if followers_num_match else None
        except Exception as e:
            print("Error occurred while getting number of employees:", e)

        # Extract additional company details safely
        try:
            company_details = response.css('.core-section-container__content .mb-2')
            company_item['website'] = company_details[0].css('a::text').get(default='').strip()
            company_item['industry'] = company_details[1].css('.text-md::text').getall()[1].strip()
            company_item['company_size_approx'] = company_details[2].css('.text-md::text').getall()[1].strip().split()[0]
            headquarters = company_details[3].css('.text-md::text').getall()
            company_item['headquarters'] = headquarters[1].strip() if headquarters[0].lower().strip() == 'headquarters' else 'not-found'
            company_item['type'] = company_details[4].css('.text-md::text').getall()[1].strip()
            
            # Handle "founded" or "specialties"
            unsure_parameter = company_details[5].css('.text-md::text').getall()
            unsure_parameter_key = unsure_parameter[0].lower().strip()
            company_item[unsure_parameter_key] = unsure_parameter[1].strip()
            if unsure_parameter_key == 'founded':
                specialties = company_details[6].css('.text-md::text').getall()
                company_item['specialties'] = specialties[1].strip() if specialties[0].lower().strip() == 'specialties' else 'not-found'
            else:
                company_item['founded'] = 'not-found'
                company_item['specialties'] = 'not-found'

        except IndexError:
            print("Skipped index due to missing details")

        yield company_item

        # Request next URL
        if company_index_tracker + 1 < len(self.company_pages):
            next_url = self.company_pages[company_index_tracker + 1]
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_response,
                meta={'company_index_tracker': company_index_tracker + 1}
            )
