import json
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
                company_urls.append(v+"/?trk=companies_directory")
            #company_urls = company_urls[:500]
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
        'DOWNLOAD_DELAY': 0.7, 
        'RETRY_TIMES': 10,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter'
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        get_url_by_company_name()

        if not company_urls:
            print("No company URLs found. Exiting spider.")
            raise ValueError("No URLs to scrape.")

        self.company_pages = list(set(company_urls.copy()))
        print(f"Found {len(self.company_pages)} urls to scrap")

    def start_requests(self):
        company_index_tracker = 0

        first_url = self.company_pages[company_index_tracker]
        yield scrapy.Request(url=first_url, callback=self.parse_response,
                             meta={'company_index_tracker': company_index_tracker,'company_url': first_url})

    def parse_response(self, response):
        company_index_tracker = response.meta['company_index_tracker']
        company_url = response.meta['company_url']
        
        # Check for redirect responses
        if response.status in [301, 302, 303, 307, 308]:
            print(f"Redirect encountered for {response.url}. Handling redirect.")
            yield scrapy.Request(url=response.headers['Location'].decode('utf-8'), 
                                 callback=self.parse_response, 
                                 meta={'company_index_tracker': company_index_tracker, 'company_url': company_url},
                                 dont_filter=True)
            return
        
        if response.status == 404:
            print(f"Skipping {str(company_index_tracker + 1)} of {str(len(self.company_pages))}")
        else:
            print('********')
            print(
                f'Scraping page: {str(company_index_tracker + 1)} of {str(len(self.company_pages))}')
            print('********')

            company_item = {}
            company_item['company_url'] = company_url
            # Get company name or set to 'not-found' if not present
            company_item['company_name'] = response.css('.top-card-layout__entity-info h1::text').get(default='not-found').strip()

            # Safely parse the followers count, default to None if conversion fails
            followers_count_text = response.xpath('//h3[contains(@class, "top-card-layout__first-subline")]/span/following-sibling::text()').get(default='not-found')
            try:
                company_item['linkedin_followers_count'] = int(followers_count_text.split()[0].strip().replace(',', '')) if followers_count_text != 'not-found' else None
            except (ValueError, AttributeError):
                company_item['linkedin_followers_count'] = None

            # Get company logo URL, default to 'not-found' if not present
            company_item['company_logo_url'] = response.css('div.top-card-layout__entity-image-container img::attr(data-delayed-url)').get(default='not-found')

            # Get "about us" section, default to 'not-found' if not present
            company_item['about_us'] = response.css('.core-section-container__content p::text').get(default='not-found').strip()

            try:
                followers_num_match = re.findall(r'\d{1,3}(?:,\d{3})*',
                                                 response.css('a.face-pile__cta::text').get(default='not-found').strip())
                if followers_num_match:
                    company_item['num_of_employees'] = int(
                        followers_num_match[0].replace(',', ''))
                else:
                    company_item['num_of_employees'] = response.css('a.face-pile__cta::text').get(
                        default='not-found').strip()
            except Exception as e:
                print("Error occurred while getting number of employees: {e}")

            try:
                company_details = response.css('.core-section-container__content .mb-2')
                try:
                    company_item['website'] = company_details[0].css('a::text').get(default='not-found').strip()
                except:
                    company_item['website'] = ""
                try:
                    company_industry_line = company_details[1].css('.text-md::text').getall()
                    company_item['industry'] = company_industry_line[1].strip()
                except:
                    company_item['industry'] = ""
                try:
                    company_size_line = company_details[2].css('.text-md::text').getall()
                    company_item['company_size_approx'] = company_size_line[1].strip().split()[0]
                except:
                    company_item['company_size_approx'] = ""
                try:
                    company_headquarters = company_details[3].css('.text-md::text').getall()
                    if company_headquarters[0].lower().strip() == 'headquarters':
                        company_item['headquarters'] = company_headquarters[1].strip()
                    else:
                        company_item['headquarters'] = 'not-found'
                except:
                    company_item['headquarters'] = ""
                try:
                    company_type = company_details[4].css('.text-md::text').getall()
                    company_item['type'] = company_type[1].strip()
                except:
                    company_item['type'] = ""
                try:
                    # specialities or founded, one among them -> storing in `unsure_parameter`
                    unsure_parameter = company_details[5].css('.text-md::text').getall()
                    unsure_parameter_key = unsure_parameter[0].lower().strip()
                    company_item[unsure_parameter_key] = unsure_parameter[1].strip()
                    # `founded` comes before specialties if exists, or else `specialties` at first means that `founded` parameter isn't defined
                    if unsure_parameter_key == 'founded':
                        company_specialties = company_details[6].css('.text-md::text').getall()
                        # after `founded` is extracted, check if `specialties` is defined
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

        if (company_index_tracker <= len(self.company_pages) - 1):
            next_url = self.company_pages[company_index_tracker]
            yield scrapy.Request(url=next_url, callback=self.parse_response,
                                 meta={'company_index_tracker': company_index_tracker, 'company_url': company_url})
