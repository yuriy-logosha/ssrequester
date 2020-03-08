#!/usr/bin/env python3
import pymongo, logging, time
import datetime, os
from utils import json_from_file, MyHTMLParser, json_to_file, _get

config_file_name = 'config.json'
config = {}

try:
    config = json_from_file(config_file_name, "Can't open ss-config file.")
except RuntimeError as e:
    print(e)
    exit()

if not os.path.exists('requests'):
    os.makedirs('requests')

formatter = logging.Formatter(config['logging.format'])
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler(config['logging.file'])

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging_level = config["logging.level"] if 'logging.level' in config else 20
print("Selecting logging level", logging_level)
print("Selecting logging format", config["logging.format"])
print("Selecting logging file \"%s\"" % config['logging.file'])

logging.basicConfig(format=config["logging.format"], handlers=[c_handler, f_handler])
logger = logging.getLogger(config["logging.name"])
logger.setLevel(logging_level)


def extract_pages(data):
    pages = []
    for line in data:
        if isinstance(line, tuple) and line[0] == 'a' and len(line[1]) == 4 and line[1][0][1] == 'nav_id':
            pages.append(line[1][3][1])
    return pages, pages.pop(0)


def is_item(item):
    return len(item) >= 3 and item[0] == 'td' and len(item[1]) > 0 and len(item[1][0]) > 1 and item[1][0][1] == config[
        "sscom.class"]


def is_url(item):
    return len(item) >= 3 and item[0] == 'a' and len(item[1]) > 2 and len(item[1][2]) > 1 and item[1][2][1] == config[
        "sscom.class.url"]


def generate_report(ads={}, new_ads=[], new_address=[]):
    try:
        for a in ads:
            for i in ads[a]['items']:
                print("{0:>30} {1:7}".format(a, str(i)))
        print("______________________________________________________")
        print("_____________  New Records  __________________________")
        for a in new_ads:
            print(a)
        print("______________________________________________________")
        print(len(new_ads), "New records found.")

        print("______________________________________________________")
        print("_____________  New Address not in GeoData DB  ________")
        for a in new_address:
            print(a)
        print("______________________________________________________")
        print(len(new_address), "New records found.")
    except RuntimeError as e:
        logger.error(e)


def uload_new_records(new_ads):
    try:
        ss_ads.ads.insert_many(new_ads)
    except RuntimeError as e:
        logger.error(e)


def export_to_file(ads):
    try:
        ads_for_json = ads.copy()
        for a in ads_for_json:
            for i in ads_for_json[a]['items']:
                i['date'] = str(i['date'])
        json_to_file(config['export.filename'], ads_for_json)
    except RuntimeError as e:
        logger.error(e)


def request_ss_records():
    data = []
    try:
        for url in config["sites"]:
            logger.info(f"Looking for new records in {url}")
            page = MyHTMLParser({'valid_tags': ['tr', 'td', 'a']}).feed_and_return(_get(url).text)
            pages, last = extract_pages(page.data)
            data += page.data
            pages_max = last.split('page')[1].split('.')[0]

            for p in range(2, int(pages_max)):
                _url = f"https://{config['sscom.url']}{last.replace(pages_max, str(p))}"
                logger.debug(f"Looking for new records in rest of pages {_url}")
                data += MyHTMLParser({'valid_tags': ['tr', 'td', 'a', 'br']}).feed_and_return(_get(_url).text).data
    except RuntimeError as e:
        logger.debug(e)
    return data


def build_db_record(items):
    a = {}
    try:
        a = {'url': config['sscom.url'] + items[0], 'address': items[1],
             'date': datetime.datetime.utcnow()}
        if len(items) == 6:
            a.update({'m2': items[2], 'level': items[3], 'type': config['house.marker'],
                 'price_m2': items[4], 'price': items[5]})
        elif len(items) == 8:
            a.update({'rooms': items[2], 'm2': items[3], 'level': items[4], 'type': items[5],
                 'price_m2': items[6], 'price': items[7]})
    except RuntimeError as e:
        logger.debug(e)
    return a


def verify_address(url, address):
    logger.debug(f"Verifying {address} url: {url}")
    return list(ss_ads.ads.find({"url": f"{url}", "address": f"{address}"}))


def verify_geodata(address):
    logger.debug(f"Verifying Geodata: {address}")
    return list(ss_ads.geodata.find({"address": f"{address}"}))


def is_property(param: str) -> bool:
    return param in config and config[param]


while True:
    try:
        myclient = pymongo.MongoClient(config["db.url"])

        with myclient:
            ss_ads = myclient.ss_ads

            data = request_ss_records()

            ads = {}
            new_ads = []
            new_address = []
            items = []
            i = 0
            while i <= len(data) - 1:
                d = data[i]
                if is_url(d) or is_item(d):
                    if is_url(d):
                        items.append(d[1][3][1])
                    elif is_item(d):
                        items.append(d[len(d) - 1])
                else:
                    if items:
                        a = build_db_record(items)
                        items = []

                        if is_property('upload') and not verify_address(a['url'], a['address']):
                            new_ads.append(a)

                        if is_property('upload') and not verify_geodata(a['address']):
                            new_address.append(a['address'])

                        try:
                            _addr = ads[a['address']]
                            _addr['items'].append(a)
                        except:
                            ads[a['address']] = {'items': [a]}

                i += 1

            if is_property('report'):
                generate_report(ads, new_ads, new_address)

            if is_property('upload') and new_ads:
                logger.info(f"Inserting new records: {len(new_ads)}")
                uload_new_records(new_ads)

            if is_property('export') and 'export.filename' in config:
                logger.info("Exporting to file: %s", config['export.filename'])
                export_to_file(ads)
    except RuntimeError as e:
        logger.error(e)

    if 'restart' in config and config['restart'] > 0:
        logger.info("Waiting %s seconds.", config['restart'])
        time.sleep(config['restart'])
    else:
        exit()
