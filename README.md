This is a parser and address normalizer for the builk data from [OpenStates](http://openstates.org/downloads/)

A large portion of this code is devoted to taking unstructred address strings and turning them into
structured data with the help of [TIGER](http://www.census.gov/geo/maps-data/data/tiger.html) and [http://federalgovernmentzipcodes.us/](http://federalgovernmentzipcodes.us/)

# Running

To run, a database user and name must be supplied. See the example config file for the format if you choose to use a config file.
The command line options override the config file.

    $ python3 create-csv.py -h
    Usage: create-csv.py [options]

    Options:
      -h, --help            show this help message and exit
      -u USER, --user=USER  User to log into the database as
      -D DB_NAME, --db=DB_NAME
                            Database to log into
      -d DB_HOST, --host=DB_HOST
                            Database host to log into (Default: localhost)
      -v VERBOSITY, --verbosity=VERBOSITY
                            How much should I complain? OFF, CRITICAL, ERROR,
                            WARNING, INFO or DEBUG (Default: DEBUG)
      -c CONFIG, --config=CONFIG
                            Location of a config file
      -o OUTFILE, --outfile=OUTFILE
                            Name of the output file (Default: ./legislators.csv)
      -l LOGFILE, --logfile=LOGFILE
                            Name of the log file (Default: -)
      -i INDIR, --indir=INDIR
                            Location of the input directory (Default:
                            ./openstates.org/legislators)

