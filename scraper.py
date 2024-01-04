import os
os.environ["SCRAPERWIKI_DATABASE_NAME"] = "sqlite:///data.sqlite"

import re
import scraperwiki
import logging
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

base_url = 'https://www.sorell.tas.gov.au/'
public_notices_url = base_url + 'currently-advertised-planning-applications/'
page = BeautifulSoup(scraperwiki.scrape(https://www.sorell.tas.gov.au/currently-advertised-planning-applications/), 'html.parser')

records = []
for table in page.find_all('table', class_='grid'):
    record = {
        'date_scraped': date.today().isoformat()
    }
    for tr in table.find_all('tr'):
        header = tr.find('td', class_="headerColumn").get_text()
        if not  header:
            continue
        element = tr.find('td', class_="headerColumn").find_next_sibling("td")

        if header == 'Application ID':
            record['Application Number'] = element.find('a').get_text()
            record['info_url'] = public_notice_details_url + record['council_reference']
        elif header == 'Application Description':
            record['Description'] = element.get_text()
        elif header == 'Property Address':
            record['Address'] = re.sub(r'\sTAS\s+(7\d{3})$', r', TAS, \1', element.get_text())

    records.append(record)

log.info(f"Found {len(records)} public notices")

for record in records:
    try:
        rs = scraperwiki.sqlite.select("* from data where council_reference=?", (record['council_reference'],))
        if rs:
            continue
    except Exception as e:
        if not 'no such table' in str(e): # happens on very first record only
            raise e

    log.info(f"Scraping Public Notice - Application Details for {record['council_reference']}")
    page = BeautifulSoup(scraperwiki.scrape(record['info_url']), 'html.parser')

    for table in page.find_all('table', class_='grid'):
        for tr in table.find_all('tr'):
            try:
                header_element = tr.find('td', class_="headerColumn")
                if not header_element:
                    continue
                header = header_element.get_text()
                value = tr.find('td', class_="headerColumn").find_next_sibling("td").get_text()
                if value == '\xa0':
                    # empty cell containing only &nbsp;
                    continue
                elif header == "Property Legal Description":
                    record['address'] = value
                elif header == "Application Received":
                    record['date_received'] = datetime.strptime(value, '%d/%m/%Y').date().isoformat()
                elif header == "Advertised On":
                    record['on_notice_from'] = datetime.strptime(value, '%d/%m/%Y').date().isoformat()
                elif header == "Advertised Close":
                    record['closes'] = datetime.strptime(value, '%d/%m/%Y').date().isoformat()
            except Exception as e:
                raise e

    scraperwiki.sqlite.save(unique_keys=['council_reference'], data=record, table_name="data")
