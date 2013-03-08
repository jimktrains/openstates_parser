#!/bin/sh

wget --random-wait --wait=5 -r -l1 -H -t1 -nd -N -np -A zip -erobots=off http://openstates.org/downloads/
cd openstates.org
ls *.zip | xargs -n 1 unzip
cd ..
