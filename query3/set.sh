#!/bin/sh
mongoimport --db air --collection flights --type csv --headerline --file ./flights.csv
mongoimport --db air --collection airlines --type csv --headerline --file ./airlines.csv
mongoimport --db air --collection airports --type csv --headerline --file ./airports.csv
mongoimport --db air --collection cancellations --type csv --headerline --file ./cancellations.csv