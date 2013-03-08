import json
import csv
import os
import urllib.parse
import urllib.request
import re
import time
import psycopg2
conn = psycopg2.connect("dbname=ls user=jim")
cur = conn.cursor()

def string_to_address(address)
    leg['office_address'] = re.sub(r'\s+', ' ', leg['office_address'])
    leg['office_address'] = leg['office_address'].replace(',', '')
    matches = re.finditer(r'((\d{5})(-\d{4})?)', leg['office_address'])
    good_zip_found = False
    for pos_zip in matches:
       if pos_zip.start() < 10:
          continue
       good_zip_found = pos_zip
       if pos_zip.group(3) is not None:
          break
    if not good_zip_found:
       print(leg['office_address'])
       print("No good zipcode match found", f)
       continue
    matches = pos_zip
    leg['office_zip'] = matches.group(1)
    zip5 = matches.group(2)
    zip4 = matches.group(3)
    cur.execute("SELECT * FROM zips WHERE zipcode = %s;", [zip5])
    for zip_city in cur:
       pos = leg['office_address'].upper().rfind(zip_city[3].upper(), 0, matches.start())
       if pos != -1:
          leg['office_state'] = zip_city[4]
          leg['office_city'] = leg['office_address'][pos:pos+len(zip_city[3])]
          leg['office_street'] = leg['office_address'][:pos-1] #rm the space
          break
       else:
          mod_city = re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', zip_city[3])).upper().replace('SAINT', 'ST').replace('CENTER', 'CTR').replace('JUNCTION', 'JCT')
          pos = re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', leg['office_address'])).upper().rfind(mod_city.upper(), 0, matches.start())
          if pos != -1:
             leg['office_state'] = zip_city[4]
             leg['office_city'] = leg['office_address'][pos:pos+len(mod_city)]
             leg['office_street'] = leg['office_address'][:pos-1] #rm the space
             break
    if leg.get('office_city') is None:
       cur.execute("SELECT * FROM zips WHERE zipcode = %s;", [zip5])
       for zip_city in cur:
          print(zip_city[3])
          print(re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', zip_city[3])).upper().replace('SAINT', 'ST'))
       print(leg['office_address'])
       print("No city found", f)
       continue
    leg['office_street'] = leg['office_street'].replace('P.O.', 'PO').\
                                replace('P O', 'PO').\
                                replace('P. O.', 'PO').\
                                replace('BOX', 'Box')
    if leg.get('phone') is None:
       phone_match = re.finditer(r'((Work)|(Cell)|(Session))\s*-?\s*(\(?\d{3}\)?\s*\d{3}-?\d{4})', leg['office_address'])
       for pos_phone in phone_match:
          if pos_phone.start() > matches.end():
             leg['phone'] =  re.sub(r'\D', '', pos_phone.group(5))

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
   for root, dirs, filenames in os.walk('legislators'):
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

            out.writerow(leg)


               # Just until I get this in git
               #print("Real address, skipping")
               #continue
               #url = "http://maps.googleapis.com/maps/api/geocode/json?"
               #url = url + urllib.parse.urlencode({'address': leg['office_address'], 'sensor':'true'})
               #res = urllib.request.urlopen(url)
               #ret = res.readall()
               #enc = res.headers.get_content_charset()
               #ecd = ret.decode(enc)
               #geocoded = json.loads(ecd)
               ##time.sleep(2)
               #if 'results' in geocoded:
               #   geocoded = geocoded['results']
               #   if len(geocoded):
               #      geocoded = geocoded[0]
               #      if 'address_components' in geocoded:
               #         for part in geocoded['address_components']:
               #            if 'street_number' in part['types']:
               #               leg['office_street_num'] = part['short_name']
               #            elif 'route' in part['types']:
               #               leg['office_street'] = part['short_name']
               #            elif 'locality' in part['types']:
               #               leg['office_city'] = part['short_name']
               #            elif 'administrative_area_level_1' in part['types']:
               #               leg['office_state'] = part['short_name']
               #            elif 'postal_code' in part['types']:
               #               leg['office_zip'] = part['short_name']
