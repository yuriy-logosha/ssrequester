#!/usr/bin/env python3
import datetime
import logging
import os
import time
from datetime import datetime
from enum import Enum

import pymongo
from myfile import json_from_file, json_to_file
from myparser import MyHTMLParser
from myrequests import get as _get

config_file_name = 'config.json'
config = {}

try:
    config = json_from_file(config_file_name, "Can't open ss-config file.")
except RuntimeError as e:
    print(e)
    exit()

if not os.path.exists('requests'):
    os.makedirs('requests')


class ADTYPE(Enum):
    UNDEFINED = 0
    NEW = 1
    OUTDATED = 2
    EXISTS = 3


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

address_field = config['address.field']


def extract_pages(data):
    pages = []
    for line in data:
        if isinstance(line, tuple) and line[0] == 'a' and len(line[1]) == 4 and line[1][0][1] == 'nav_id':
            pages.append(line[1][3][1])
    if pages:
        return pages.pop(0)
    else:
        return 'page1.html'


def is_item(item):
    return len(item) >= 3 and item[0] == 'td' and len(item[1]) > 0 and len(item[1][0]) > 1 and item[1][0][1] == config[
        "sscom.class"]


def is_url(item):
    return len(item) >= 2 and item[0] == 'a' and len(item[1]) > 2 and len(item[1][2]) > 1 and item[1][2][1] == config[
        "sscom.class.url"]


def generate_report(ads={}, new_ads=[], outdated_ads=[]):
    try:
        print("____________ Retrieved from remote  __________________")
        for a in ads:
            for i in ads[a]['items']:
                print("{0:>30} {1:7}".format(a, str(i)))
        print("______________________________________________________")
        print("_____________  New Ad Records  _______________________")
        for a in new_ads:
            print(a)
        print(len(new_ads), "New ad records found.")
        print("_____________  Outdated Records  _____________________")
        for a in outdated_ads:
            print(a)
        print(len(outdated_ads), "Outdated records found.")
    except RuntimeError as e:
        logger.error(e)


def upload_new_records(new_ads):
    try:
        ss_ads.ads.insert_many(new_ads)
    except RuntimeError as e:
        logger.error(e)


def update_outdated_records(ads):
    try:
        for r in ads:
            ss_ads.ads.update_one({'_id': r[0]}, {'$set': {'outdated': False}})
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
            page = MyHTMLParser(config["sscom.parser.config"]).feed_and_return(_get(url).text)
            last = extract_pages(page.data)
            data += page.data
            pages_max = last.split('page')[1].split('.')[0]

            for p in range(2, int(pages_max) + 1):
                _url = f"{config['sscom.url']}{last.replace(pages_max, str(p))}"
                logger.debug(f"Looking for new records in rest of pages {_url}")
                data += MyHTMLParser(config["sscom.parser.config"]).feed_and_return(_get(_url).text).data
    except RuntimeError as e:
        logger.debug(e)
    return data


type_mapping = {
    'Hrušč.': 'Хрущ.',
    'Jaun.': 'Нов.',
    'Renov.': 'Рекон.',
    'LT proj.': 'Лит. пр.',
    'Specpr.': 'Спец. пр.',
    'Staļina': 'Сталинка',
    '103.': '103-я',
    '104.': '104-я',
    '119.': '119-я',
    '467.': '467-я',
    '602.': '602-я',
    'M. ģim.': 'М. сем.',
    'Priv. m.': 'Ч. дом',
    'P. kara': 'Дов. дом',
    'Čehu pr.': 'Чеш. пр.'
}

room_mapping = {
    'Citi': '-'
}


def get_type_mapping(key):
    try:
        return type_mapping[key]
    except:
        return key


def get_room_mapping(key):
    try:
        return room_mapping[key]
    except:
        return key


def build_db_record(items):
    a = {}
    try:
        a = {'kind': 'ad', 'url': '/'.join(items[0].split('/')[3:]), address_field: items[1], 'date': datetime.utcnow()}
        if len(items) == 6:
            a.update({'m2': items[2], 'level': items[3], 'type': get_type_mapping('Priv. m.'), 'price_m2': items[4],
                      'price': items[5]})
        elif len(items) == 8:
            a.update({'rooms': get_room_mapping(items[2]), 'm2': items[3], 'level': items[4],
                      'type': get_type_mapping(items[5]), 'price_m2': items[6], 'price': items[7]})
    except RuntimeError as e:
        logger.debug(e)
    return a


def verify_ad(url, address):
    try:
        ad = list(ss_ads.ads.find({"url": f"{url}", address_field: f"{address}"}))
        if len(ad) == 0:
            return ADTYPE.NEW, None
        elif len(ad) == 1:
            if 'outdated' in ad[0] and ad[0]['outdated']:
                return ADTYPE.OUTDATED, ad[0]['_id']
            return ADTYPE.EXISTS, None
    except Exception as e:
        logger.error(e)

    return ADTYPE.UNDEFINED, None


def verify_geodata(address):
    return list(ss_ads.geodata.find({'address': f"{address}"}))


def is_property(param: str) -> bool:
    return param in config and config[param]


def to_buffer(buffer, d):
    if is_url(d):
        buffer.append(d[1][3][1])
    elif is_item(d):
        buffer.append(d[len(d) - 1])


def to_ads(ads, a):
    try:
        _addr = ads[a[address_field]]
        _addr['items'].append(a)
    except:
        ads[a[address_field]] = {'items': [a]}


while True:
    try:
        myclient = pymongo.MongoClient(config["db.url"])

        with myclient:
            ss_ads = myclient.ss_ads
            data = request_ss_records()

            ads, new_ads, outdated_ads, buffer, i = {}, [], [], [], 0
            while i < len(data):
                d = data[i]
                if is_url(d) or is_item(d):
                    to_buffer(buffer, d)
                elif buffer:
                    a, buffer = build_db_record(buffer), []

                    strategy, addit = verify_ad(a['url'], a[address_field])
                    logger.debug(f"Verify AD: {strategy} {a[address_field]} url: {a['url']}")
                    if strategy == ADTYPE.NEW:
                        new_ads.append(a)
                    if strategy == ADTYPE.OUTDATED:
                        outdated_ads.append((addit, a))

                    to_ads(ads, a)

                i += 1

            print(len(new_ads), "New ad records found.")
            print(len(outdated_ads), "Outdated records found.")

            if is_property('report'):
                generate_report(ads, new_ads, outdated_ads)

            if is_property('upload') and new_ads:
                logger.info(f"Inserting new records: {len(new_ads)}")
                upload_new_records(new_ads)

            if is_property('upload') and outdated_ads:
                logger.info(f"Updating records: {len(outdated_ads)}")
                update_outdated_records(outdated_ads)

            if is_property('export') and 'export.filename' in config:
                logger.info("Exporting to file: %s", config['export.filename'])
                export_to_file(ads)
    except RuntimeError as e:
        logger.error(e)

    if 'restart' in config and config['restart'] > 0:
        logger.info("Waiting %s seconds.", config['restart'])
        time.sleep(config['restart'])
    else:
        break
