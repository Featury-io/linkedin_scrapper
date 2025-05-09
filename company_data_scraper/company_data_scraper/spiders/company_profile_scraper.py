import json
import time
from typing import Any, Iterable
import scrapy
from scrapy.http import Request, Response
import re
from company_data_scraper.pipelines import CompanyProfilePipeline
import csv

#input_file = 'company_names.json'
input_file = 'company_ids.csv'
company_urls = []

def get_url_by_company_name():
    global company_urls
    try:
        with open(input_file, 'r') as json_file:
            data = json.load(json_file)
            for v in data.values():
                if isinstance(v,str) and len(v)>=1 and v[-1]=='/':
                    v=v[:-1]
                company_urls.append(v + "/?trk=companies_directory")
            print("Company URLs:", len(company_urls))
    except FileNotFoundError:
        print(f"Error: JSON file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred while reading JSON file: {str(e)}")

def get_url_by_company_id():
    global company_urls
    existing_data=[]
    try:
        output_file = 'company_profile_data.json'
        with open(output_file, 'r') as f:
            existing_data = json.load(f)
            existing_data = [item['company_url'] for item in existing_data]
            print("Found ", len(existing_data), " existing company URLs")
    except:
        print("No existing data found")
    try:
        with open(input_file, 'r') as csv_file:
            reader = csv.reader(csv_file)   
            for row in reader:
                url="https://www.linkedin.com/company/" + row[0] + "/?trk=companies_directory"
                if url not in existing_data:
                    company_urls.append(url)
            print(f" >  Loaded {len(company_urls)} Company URLs")
    except FileNotFoundError:
        print(f"/!\ Error: CSV file '{input_file}' not found.")
    except Exception as e:
        print(f"/!\ Error: An error occurred while reading CSV file: {str(e)}")


class CompanyProfileScraperSpider(scrapy.Spider):
    name = 'company_profile_scraper'
    
    custom_settings = {
        'HTTPERROR_ALLOW_ALL': True,
        'REDIRECT_ENABLED': True,
        'REDIRECT_MAX_TIMES': 5,
        'DOWNLOAD_DELAY': 2,              # Increased download delay to 3 seconds
        'RANDOMIZE_DOWNLOAD_DELAY': True, # Randomize download delay for natural behavior
        'RETRY_TIMES': 5,
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.RFPDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #get_url_by_company_name()
        get_url_by_company_id()
        print(f" >  Found {len(company_urls)} URLs to scrape. Have to check if they are already scraped")

        self.company_pages = [url for url in set(company_urls) if url not in CompanyProfilePipeline.scraped_urls]
        print(f" >  Found {len(self.company_pages)} new URLs to scrape.")
        
        if not company_urls:
            print(" >  No company URLs found. Exiting spider.")
            raise ValueError(" >  No URLs to scrape.")

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
                meta={'company_index_tracker': company_index_tracker, 'retry_count': retry_count, 'cookiejar': retry_count},
                dont_filter=True
            )
            return

        # Retry on 404 errors
        if response.status == 404:
            if retry_count < 2:
                print(f"Page not found for {company_url}. Retrying ({retry_count + 1}/3)...")
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
        print(f'Scraping page: {str(company_index_tracker + 1)} of {str(len(self.company_pages))} - URL QUERIED: {company_url} -  CURRENT URL: {response.url}')
        print('********')

        company_item = {}
        company_item['company_url'] = company_url

        company_item['company_name'] = response.css('.top-card-layout__entity-info h1::text').get(default='not-found').strip()

        # Pause for an additional 3 seconds if company_name is 'not-found'
        if company_item['company_name'] == 'not-found':
            print("Company name not found. Skipping it for now - ", company_url)
            return

        followers_count_text = response.xpath('//h3[contains(@class, "top-card-layout__first-subline")]/span/following-sibling::text()').get(default='not-found')
        try:
            company_item['linkedin_followers_count'] = int(followers_count_text.split()[0].strip().replace(',', '')) if followers_count_text != 'not-found' else None
        except (ValueError, AttributeError):
            company_item['linkedin_followers_count'] = None

        company_item['company_logo_url'] = response.css('div.top-card-layout__entity-image-container img::attr(data-delayed-url)').get(default='not-found')
        company_item['about_us'] = response.css('.core-section-container__content p::text').get(default='not-found').strip()

        try:
            #followers_num_match = re.findall(r'\d{1,3}(?:,\d{3})*', response.css('a.face-pile__cta::text').get(default='not-found').strip())
            followers_text=response.css('p.face-pile__text::text').get(default='not-found').strip() # 'View all 62 employees'
            followers_num_match = re.findall(r'\d{1,3}(?:,\d{3})*', followers_text)
            company_item['num_of_employees'] = int(followers_num_match[0].replace(',', '')) if followers_num_match else None
        except Exception as e:
            print("Error occurred while getting number of employees:", e)

        # Extract additional company details safely
        try:
            company_details = response.css('.core-section-container__content .mb-2')
            try:
                company_item['website'] = company_details[0].css('a::text').get(default='not-found').strip()
            except:
                company_item['website'] = ""

            company_item['industry'] = ""
            company_item['company_size_approx'] = ""
            company_item['headquarters'] = ""
            company_item['type'] = ""
            company_item['founded'] = ""

            for i in range(len(company_details)):
                try:
                    company_detail_item=company_details[i].css('.text-md::text').getall()
                    if company_detail_item[0].lower().strip() == 'industry':
                        company_item['industry'] = company_detail_item[1].strip()
                    elif company_detail_item[0].lower().strip() == 'company size':
                        company_item['company_size_approx'] = company_detail_item[1].strip().split()[0]
                    elif company_detail_item[0].lower().strip() == 'headquarters':
                        company_item['headquarters'] = company_detail_item[1].strip()
                    elif company_detail_item[0].lower().strip() == 'type':
                        company_item['type'] = company_detail_item[1].strip()
                    elif company_detail_item[0].lower().strip() == 'founded':
                        company_item['founded'] = company_detail_item[1].strip()
                except:
                    pass
            try:
                # specialities or founded, one among them -> storing in unsure_parameter
                unsure_parameter = company_details[5].css('.text-md::text').getall()
                unsure_parameter_key = unsure_parameter[0].lower().strip()
                company_item[unsure_parameter_key] = unsure_parameter[1].strip()
                # founded comes before specialties if exists, or else specialties at first means that founded parameter isn't defined
                if unsure_parameter_key == 'founded':
                    company_specialties = company_details[6].css('.text-md::text').getall()
                    # after founded is extracted, check if specialties is defined
                    if company_specialties[0].lower().strip() == 'specialties':
                        company_item['specialties'] = company_specialties[1].strip()
                    else:
                        company_item['specialties'] = 'not-found'
                elif unsure_parameter_key != 'specialties' or unsure_parameter_key == 'founded':
                    company_item['founded'] = 'not-found'
                    company_item['specialties'] = 'not-found'
            except:
                company_item['founded'] = ""
                company_item['specialties'] = ""

        except IndexError:
            print("Error: *****Skipped index, as some details are missing*********")

        yield company_item
        company_index_tracker += 1

        # Request next URL
        if company_index_tracker + 1 < len(self.company_pages):
            next_url = self.company_pages[company_index_tracker + 1]
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_response,
                meta={'company_index_tracker': company_index_tracker + 1}
            )


# scrapy  crawl company_profile_scraper