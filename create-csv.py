#!/usr/bin/env python3
from itertools import *
from optparse import OptionParser
import configparser
import csv
import getpass
import jellyfish
import json
import logging
import os
import psycopg2
import re
import sys
import time
import urllib.parse
import urllib.request

opts = OptionParser()
opts.add_option('-u', '--user', dest='db_user', help="User to log into the database as",metavar="USER")
opts.add_option('-D', '--db', dest='db_name', help="Database to log into",metavar="DB_NAME")
opts.add_option('-d', '--host', dest='db_host', help="Database host to log into (Default: localhost)",metavar="DB_HOST")
opts.add_option('-p', '--password', dest='db_pass', action="store_true", help="If present, the program will prompt you for a database password (Default: False)")
opts.add_option('-v', '--verbosity', dest='verbosity', help="How much should I complain? OFF, CRITICAL, ERROR, WARNING, INFO or DEBUG (Default: DEBUG)", metavar="VERBOSITY")
opts.add_option('-c', '--config', dest='config', help="Location of a config file",metavar="CONFIG")
opts.add_option('-o', '--outfile', dest='outfile', help="Name of the output file (- is stdout) (Default: ./legislators.csv)",metavar="OUTFILE")
opts.add_option('-l', '--logfile', dest='logfile', help="Name of the log file (- is stderr) (Default: -)",metavar="LOGFILE")
opts.add_option('-i', '--indir', dest='indir', help="Location of the input directory (Default: ./openstates.org/legislators)",metavar="INDIR")

(options, args) = opts.parse_args()

if options.config is not None:
   config = configparser.SafeConfigParser()
   if 0 == len(config.read(options.config)):
      print("Config file '%s' is not valid" % options.config)
      exit()
   if 'database' in config:
      if 'user' in config['database'] and options.db_user is None: options.db_user = config['database']['user']
      if 'host' in config['database'] and options.db_host is None: options.db_host = config['database']['host']
      if 'name' in config['database'] and options.db_name is None: options.db_name = config['database']['name']
      if 'ask_password' in config['database'] and options.db_pass is None: options.db_pass = 'True' == config['database']['ask_password']
   if 'logging' in config:
      if 'level' in config['logging'] and options.verbosity is None:  options.verbosity = config['logging']['level']
      if 'file' in config['logging'] and options.logfile is None: options.logfile = config['logging']['file']
   if 'parsing' in config:
      if 'indir' in config['parsing'] and options.indir is None: options.indir = config['parsing']['indir']
      if 'outfile' in config['parsing'] and options.outfile is None: options.outfile = config['parsing']['outfile']

if options.db_host is None: options.db_host = 'localhost'
if options.verbosity is None:  options.verbosity = 'DEBUG'
if options.logfile is None: options.logfile = '-'
if options.indir is None: options.indir = './openstates.org/legislators'
if options.outfile is None: options.outfile = './legislators.csv'

if options.db_pass:
   options.db_pass = getpass.getpass(prompt="Database Password for %s:" % options.db_user)

# Doesn't quite work yet....
if options.outfile == '-':
   print("This option doesn't work yet")
   exit()
   options.outfile = sys.stdout

# Convert log levels and file into
# a logger
log_levels = {
   "OFF": None,
   "CRITICAL": logging.CRITICAL,
   "ERROR":  logging.ERROR,
   "WARNING": logging.WARNING,
   "INFO":  logging.INFO,
   "DEBUG": logging.DEBUG
}
if options.verbosity not in log_levels:
   print("%s is not a valid log level" % options.verbosity)
   exit()
options.verbosity = log_levels[options.verbosity]
hdlr = logging.NullHandler()
if options.verbosity is None:
   options.verbosity = logging.CRITICAL
if options.logfile == '-':
   options.logfile = sys.stderr
   hdlr = logging.StreamHandler(options.logfile)
else:
   hdlr = logging.FileHandler(options.logfile)

# Create our logger
# Yes, we should make this local and pass it around...
# Modified from
# http://docs.python.org/3/library/logging.html
logger = logging.getLogger('openstates')
formatter = logging.Formatter('%(cur_file)s %(message)s')

hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(options.verbosity)

# Create a global db connection
# Yes, we should make this local and pass it around...
conn = psycopg2.connect(database=options.db_name, user=options.db_user, password=options.db_pass)
cur = conn.cursor()

# Taken from
# http://docs.python.org/2/library/itertools.html#recipes
def powerset(iterable):
   "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
   s = list(iterable)
   return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

# Makes a call to Google's geocoding service
def check_google_because_I_give_up(raw_address):
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
   url = "http://maps.googleapis.com/maps/api/geocode/json?"
   url = url + urllib.parse.urlencode({'address': raw_address, 'sensor':'false'})
   res = urllib.request.urlopen(url)
   ret = res.readall()
   enc = res.headers.get_content_charset()
   ecd = ret.decode(enc)
   geocoded = json.loads(ecd)
   if 'results' in geocoded:
      geocoded = geocoded['results']
      if len(geocoded):
         geocoded = geocoded[0]
         if 'address_components' in geocoded:
            for part in geocoded['address_components']:
               if 'street_number' in part['types']:
                  address['street_num'] = part['short_name']
               elif 'route' in part['types']:
                  address['street'] = part['short_name']
               elif 'locality' in part['types']:
                  address['city'] = part['short_name']
               elif 'administrative_area_level_1' in part['types']:
                  address['state'] = part['short_name']
               elif 'postal_code' in part['types']:
                  address['zip'] = part['short_name']
   return address


def zipcode_to_city_list(zipcode):
    # This will return all of the cities that have out best-guess zipcode
    cur.execute("SELECT * FROM zips WHERE zipcode = %s;", [zipcode])
    zips = [(x[3], x[4]) for x in cur]

    # This list of lambdas are a collection of changes that
    # could be made to an address. They should accept and return
    # in all caps with no punctuation
    # This is only a small portion of the possible modifications
    # but I was only targeting common ones.
    # The list is of Lambdata, not tuples because
    # I originally had modifications that wern't simple subsitutions
    base_city_modifications = [
      lambda t : (t[0].replace('SAINT', 'ST'), t[1]),
      lambda t : (t[0].replace('CENTER', 'CTR'), t[1]),
      lambda t : (t[0].replace('JUNCTION', 'JCT'), t[1]),
      lambda t : (t[0].replace('EAST', 'E'), t[1]),
      lambda t : (t[0].replace('WEST', 'W'), t[1]),
      lambda t : (t[0].replace('NORTH', 'N'), t[1]),
      lambda t : (t[0].replace('SOUTH', 'S'), t[1])
    ]
    # Now lets take all combinations of our modifications
    # and apply them to the base city and append all the possible
    # changes to our list of cities
    city_modifications = powerset(base_city_modifications)
    mod_city_names = []
    for city_state in zips:
       city_state = (re.sub(r'\s+', ' ', re.sub(r'[^0-9a-zA-Z]',' ', city_state[0])), city_state[1])
       for mods in city_modifications:
         c = city_state
         for mod in mods:
            c = mod(c)
         mod_city_names.append(c)
    return set(sorted(zips + mod_city_names, key=lambda x : len(x[0]), reverse=True))

def string_to_address(raw_address):
    original_raw_address = raw_address
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

    # OK! Special cases time!
    # =======================
    # The data only has "_____ HOB", "___ CB", 
    # "___ Farnum Bldg", or "___ Capitol Bldg"for Michigan
    # If another state in the data starts doing then then we'll have to get creative
    #house
    if 6 == raw_address.find('HOB'):
      address['street'] = raw_address + "\n124 North Capitol Avenue\nPO Box 30014"
      address['city']   = "Lansing"
      address['state']  = "MI"
      address['zip']    =  "48909-7514"
      return address
    if 4 == raw_address.find('CB'):
      address['street'] = raw_address + "\nPO Box 30014"
      address['city']   = "Lansing"
      address['state']  = "MI"
      address['zip']    = "48909-7514"
      return address
    # Senate
    if -1 != raw_address.find("Capitol Bldg") or -1 != raw_address.find("Farnum Bldg"):
      address['street'] = raw_address + "\nPost Office Box 30036"
      address['city']   = "Lansing"
      address['state']  = "MI"
      address['zip']    = "48909-7536"
      return address

    # The data only has "Hawaii State Capitol Room ___"
    if 0 == raw_address.find("Hawaii State Capitol"):
      address['street'] = raw_address + "\n415 South Beretania St."
      address['city']   = "Honolulu"
      address['street'] = "HI"
      address['zip']    = "96813"
      return address

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
      raise Exception("No canidate zipcode found.\n\tRaw: %s" % raw_address)

    zipcode_match = canidate_zipcode

    # Store the full Zip+4
    address['zip'] = zipcode_match.group(1)
    zip5 = zipcode_match.group(2)
    zip4 = zipcode_match.group(3)

    # This is a super naive way of doing this, but
    # there were quite a few misspellings in the data
    # We create a sliding window, requiring the character
    # befor the window to be not alphanumeric (assuming
    # a space doesn't work because some people don't use spaces:-\)
    # the length of the canidate city and slide it from index 5
    # to the end. (Why 5? The city won't be that early
    # in the string.) We then take the contents of the
    # sliding window and calculator the Jaro-Winkler
    # Distance (https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance)
    # of it and the canidate.  We then take the best
    # window, and if its score is greater greater than
    # 90% (Why 90%? I felt like it?) we'll use that as
    # the city, otherwise we discard and move on.
    raw_address_upper = raw_address.upper()
    cities = zipcode_to_city_list(zip5)
    best_city_start_index = None
    best_city_name = None
    best_score = 0
    for (canidate_city, canidate_state) in cities:
       for i in range(5, zipcode_match.start()):
         if not raw_address_upper[i-1].isalnum():
            score = jellyfish.jaro_distance(canidate_city, raw_address_upper[i:i+len(canidate_city)])
            if score > best_score:
               best_city_start_index = i
               best_score = score
               best_city_name = canidate_city
    if best_score < 0.9:
       best_city_start_index = None
    if best_city_start_index:
       address['state'] = canidate_state
       address['city'] = raw_address[best_city_start_index:best_city_start_index+len(best_city_name)]
       address['street'] = raw_address[:best_city_start_index-1] #rm the space

    # At this point the failurs I'm seeing are related to
    # cities not in my zipcode <-> city database
    # I could get more creative, but I don't know if its
    # worth it right now
    if address['city'] is None:
       address = check_google_because_I_give_up(raw_address)

    # OK, I really don't know what's going on
    if address['street'] is None or address['city'] is None:
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
    # Max Length is 8 words or 40 characters per line
    # Up to now, street is a single line
    # Since the street line is the longest, it's all
    # I'll worry about here.
    # Publication 28 Section 35
    # http://pe.usps.com/text/pub28/28c3_015.htm
    if len(address['street']) > 40 or len(address['street'].split(' ')) > 8:
      terms_to_break_before = [
         "Room",
         "Rm",
         "Square",
         "Sqr",
         "Suite",
         "Ste",
         "PO Box"
      ]
      terms_to_break_after = [
         "Building",
         "District",
         "Center",
         "House of Representatives",
         "House",
         "Leader",
         "Pro Tempore",
         "Whip"
      ]
      # Some abbreviations from
      # Publication 28 Appendix G: Business Word Abbreviations
      # http://pe.usps.com/text/pub28/28apg.htm
      terms_to_switch = {
         "Building": "BLDG",
         "Station": "STA",
         "Plaza": "PLZ",
         "Memorial": "MEML",
         "Office": "OFC",
         "North": "N",
         "South": "S",
         "West": "W",
         "East": "E",
         "Avenue": "AVE",
         "Street": "ST",
         "Road": "RD",
         "Circle": "CIR",
         "Center": "CTR",
         "House": "HSE",
         "Suite": "STE",
         "Square": "SQR",
         "Capitol": "CPTOL",
         "Capital": "CPTAL",
         "Senator": "SEN",
         "Representative": "REP",
         "Leader": "LDR",
      }
      address['street'] = address['street'].replace('.', '')
      # We only want to add a preciding or trailing newline
      # if we found a word, not just a suffix or prefix,
      # and that word isn't the start or end of a line
      for term in terms_to_break_before:
         address['street'] = address['street'].replace(" %s " % term, "\n%s " % term, 1)
      for term in terms_to_break_after:
         address['street'] = address['street'].replace(" %s " % term, " %s\n" % term, 1)

      # Check if we fixed or did anything?
      # If not a last ditch effort is to replace 
      # what we can with abbreviations. We search
      # only for full words, not prefixes or suffixes.
      #
      # i.e.: Abbr. ALL THAT THINGS!
      for first_line in address['street'].split("\n"):
         if len(first_line) > 40 or len(first_line.split(' ')) > 8:
            for term in terms_to_switch:
               address['street'] = re.sub(r"(\b)%s(\b)" % term, r"\1%s\2" % terms_to_switch[term], address['street'])
    return address

with open(options.outfile, 'w') as csv_file:
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
      'state',
      'level',
      'chamber',
      'district',
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

   for root, dirs, filenames in os.walk(options.indir):
      for f in filenames:
         log_extra = {"cur_file": f}
         with open(os.path.join(root, f), 'r') as json_file:
            leg = json.load(json_file)
            leg['sunlight_id'] = leg['id']

            addys = None
            # Loop over all the available offices
            # I would prefer to use the capitol office
            # address, but if that isn't available, then
            # use what's listed first
            if len(leg['offices']):
               addys = [x for x in leg['offices'] if x['type'] == "capitol"]
               if addys is None or len(addys) == 0:
                  addys = [leg['offices'][0]]
               addys = addys[0]

               # Pull the data from the office address section
               # if it's not listed in the main section
               #
               # The last condition for the address
               # is because the longer address is probably better
               if 'office_address' not in leg or \
                  leg['office_address'] is None or \
                  len(leg['office_address']) < len(addys['address']): 
                  leg['office_address'] = addys['address']
               if leg.get('email') is None:
                  leg['email'] = addys['email']
               if leg.get('phone') is None:
                  leg['phone'] = addys['phone']
               if leg.get('fax') is None:
                  leg['fax'] = addys['fax']

            # If they don't have an address I can't use them
            if 'office_address' not in leg or leg['office_address'] is None:
               logger.warning("Could not find an address", extra=log_extra)
               continue

            # If they don't have a district, I can't use them
            if 'district' not in leg:
               logger.warning("Could not find a district", extra=log_extra)
               continue

            # I just want 10-digit phone/fax numbers, no formatting
            if 'phone' in leg and leg['phone'] is not None:
               leg['phone'] = re.sub(r'\D', '', leg['phone'])
            if 'fax' in leg and leg['fax'] is not None:
               leg['fax'] = re.sub(r'\D', '', leg['fax'])

            # I guess some email addresses are getting cut
            # off for some reason, so lets fix them
            if 'email' in leg and leg['email'] is not None:
               if leg['email'].endswith('.c'):
                  leg['email'] = leg['email'] + 'om'
               if leg['email'].endswith('.ne'):
                  leg['email'] = leg['email'] + 't'

            # Let's try to make sense of the address we have
            address = {}
            try:
               address = string_to_address(leg['office_address'])
            except Exception as e:
               logger.error(e, extra=log_extra)
               continue
            # If we made sense of the address, lets
            # import it into our legislator's record
            for part in address:
               if part == "phone":
                  leg[part] = address[part]
               else:
                  leg["office_" + part] = address[part]

            out.writerow(leg)
