import json
import csv
import os
import urllib.parse
import urllib.request
import re
import time
import psycopg2
import jellyfish
from itertools import *

conn = psycopg2.connect("dbname=ls user=jim")
cur = conn.cursor()


def powerset(iterable):
   "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
   s = list(iterable)
   return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def zipcode_to_city_list(zipcode):
    # This will return all of the cities that have out best-guess zipcode
    cur.execute("SELECT * FROM zips WHERE zipcode = %s;", [zipcode])
    zips = [(x[3], x[4]) for x in cur]

    # This list of lambdas are a collection of changes that
    # could be made to an address. They should accept and return
    # in all caps with no punctuation
    base_city_modifications = [
      lambda t : (t[0].replace('SAINT', 'ST'), t[1]),
      lambda t : (t[0].replace('CENTER', 'CTR'), t[1]),
      lambda t : (t[0].replace('JUNCTION', 'JCT'), t[1]),
      lambda t : (t[0].replace('EAST', 'E'), t[1]),
      lambda t : (t[0].replace('WEST', 'W'), t[1]),
      lambda t : (t[0].replace('NORTH', 'N'), t[1]),
      lambda t : (t[0].replace('SOUTH', 'S'), t[1])
    ]
    city_modifications = powerset(base_city_modifications)
    mod_city_names = []
    for city_state in zips:
       city_state = (re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', city_state[0])), city_state[1])
       for mods in city_modifications:
         c = city_state
         for mod in mods:
            c = mod(c)
         mod_city_names.append(c)
    return set(zips + mod_city_names)

def string_to_address(raw_address):
    address = {
      'street_number': None, # This isn't yet parsed out, but it's on the list
      'street': None, # PO Boxes will be stuck into this field, fwiw
                     # Street numbers + street are currently kept in this field
      'city': None,
      'state': None,
      'zipcode': None,
      'phone': None # Yes, Yes, this isn't part of an address, but a lot of the raw data
                  # has phone numbers in them and we need to collect them

    }

    # Commas and extra spaces are annoying
    # burn them!
    raw_address = re.sub(r'\s+', ' ', raw_address)
    raw_address = raw_address.replace(',', '')

    # All US Zipcodes are 5 digits with an optional +4 section, a hyphen followed by 4 digits
    matches = re.finditer(r'((\d{5})(-\d{4})?)', raw_address)
    good_zip_found = False
    for canidate_zipcode in matches:
       # If the canidate zipcode is closer to the start
       # of the raw address it's probably not a zipcode
       if canidate_zipcode.start() < 10:
          continue
       # Since we have a canidate not near the start, lets
       # go ahead and use it. Note that this means we're using
       # the last canidate...
       good_zip_found = canidate_zipcode
       # ...unless it's a zip+4 code, then we're pretty sure
       # we're dealing with a zipcode, so we'll just use that,
       # really
       if canidate_zipcode.group(3) is not None:
          break
    if not good_zip_found:
      raise Exception('No canidate zipcode found')

    zipcode_match = canidate_zipcode

    # Store the full Zip+4
    address['zip'] = zipcode_match.group(1)
    zip5 = zipcode_match.group(2)
    zip4 = zipcode_match.group(3)

    raw_address_upper = raw_address.upper()
    cities = zipcode_to_city_list(zip5)
    for (canidate_city, canidate_state) in cities:
       # This is a super naive way of doing this
       # We create a sliding window the length of
       # the canidate city and slide it from index 5
       # to the end. (Why 5? The city won't be that early
       # in the string.) We then take the contents of the
       # sliding window and calculator the Jaro-Winkler
       # Distance (https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
       # of it and the canidate.  We then take the best
       # window, and if its score is greater greater than
       # 90% (Why 90%? I felt like it?) we'll use that as
       # the city, otherwise we discard and move on.
       city_start_index = None
       best_score = 0
       for i in range(5, len(raw_address_upper) + zipcode_match.start() - len(canidate_city)):
         score = jellyfish.jaro_distance(canidate_city, raw_address_upper[i:i+len(canidate_city)])
         if score > best_score:
            city_start_index = i
            best_score = score
       if best_score < 0.9:
          city_start_index = None
       if city_start_index:
          address['state'] = canidate_state
          address['city'] = raw_address[city_start_index:city_start_index+len(canidate_city)]
          address['street'] = raw_address[:city_start_index-1] #rm the space
          break

    if address['city'] is None:
       raise Exception("No cities in the found zipcode match the raw address string.\n\tRaw: %s\n\tCanidates: %s" % (raw_address_upper, cities))

    # It should be PO Box, not anything else
    # Pub 28 sec 2.28
    # http://pe.usps.com/text/pub28/28c2_037.htm
    address['street'] = address['street'].replace('P.O.', 'PO').\
                                replace('P O', 'PO').\
                                replace('P. O.', 'PO').\
                                replace('BOX', 'Box')

    # Some of the raw address strings contain phone contact information as well.
    # So far they all seem to be of the type "type - number" so we're going to go
    # with that
    phone_match = re.finditer(r'((Work)|(Cell)|(Session))\s*-?\s*(\(?\d{3}\)?\s*\d{3}-?\d{4})', raw_address)
    for pos_phone in phone_match:
       if pos_phone.start() > zipcode_match.end():
          address['phone'] =  re.sub(r'\D', '', pos_phone.group(5))
    return address

with open('legislators.csv', 'w') as csv_file:
   out = csv.DictWriter(csv_file, [
      'sunlight_id',
      'votesmart_id',
      'transparencydata_id',
      'nimsp_id',
      'nimsp_candidate_id',
      'first_name',
      'middle_name',
      'last_name',
      'suffix',
      'chamber',
      'level',
      'district',
      'state',
      'office_address',
      'office_street_num',
      'office_street',
      'office_city',
      'office_state',
      'office_zip',
      'email',
      'phone',
      'fax'
   ], extrasaction='ignore')
   out.writeheader()
   for root, dirs, filenames in os.walk('openstates.org/legislators'):
      for f in filenames:
         #if not f.startswith('MN'):
         #   continue
         #print("working on: ",f)
         with open(os.path.join(root, f), 'r') as json_file:
            leg = json.load(json_file)
            leg['sunlight_id'] = leg['id']

            addys = None
            if len(leg['offices']):
               addys = [x for x in leg['offices'] if x['type'] == "capitol"]
               if addys is None or len(addys) == 0:
                  addys = [leg['offices'][0]]
               addys = addys[0]
               if 'office_address' not in leg or \
                  leg['office_address'] is None or \
                  len(leg['office_address']) < len(addys['address']):
                  leg['office_address'] = addys['address']
               if 'email' not in leg or leg['email'] is None:
                  leg['email'] = addys['email']
               if 'phone' not in leg or leg['phone'] is None:
                  leg['phone'] = addys['phone']
               if 'fax' not in leg or leg['fax'] is None:
                  leg['fax'] = addys['fax']
            if 'office_address' not in leg or leg['office_address'] is None:
               print("Could not find an address", f)
               continue
            if 'district' not in leg:
               print("Could not find a district", f)
               continue

            if 'phone' in leg and leg['phone'] is not None:
               leg['phone'] = re.sub(r'\D', '', leg['phone'])
            if 'fax' in leg and leg['fax'] is not None:
               leg['fax'] = re.sub(r'\D', '', leg['fax'])
            if 'email' in leg and leg['email'] is not None:
               if leg['email'].endswith('.c'):
                  leg['email'] = leg['email'] + 'om'
               if leg['email'].endswith('.ne'):
                  leg['email'] = leg['email'] + 't'

            try:
               address = string_to_address(leg['office_address'])
            except Exception as e:
               print(e)
               continue

            for part in address:
               leg[part] = address[part]
            out.writerow(leg)
