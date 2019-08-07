import sqlite3
import csv
con = sqlite3.connect("air.db")
cur = con.cursor()

# creat the flights table 
with open('./hw1/flights.csv','r') as flights_table:
    dr = csv.DictReader(flights_table, delimiter=',')
    cols = dr.fieldnames[:-1]
    to_db = [tuple([i[j] for j in cols]) for i in dr]

cur.execute("drop table if exists flights;")
cur.execute('''CREATE TABLE flights (
            YEAR number, MONTH number, DAY_OF_MONTH number,
            DAY_OF_WEEK number, OP_UNIQUE_CARRIER text, TAIL_NUM text, 
            OP_CARRIER_FL_NUM number, ORIGIN_AIRPORT_ID number, 
            DEST_AIRPORT_ID number, CRS_DEP_TIME number, DEP_TIME number,
            DEP_DELAY number, CRS_ARR_TIME number, ARR_TIME number, 
            ARR_DELAY number, CANCELLED number, CANCELLATION_CODE text, 
            CRS_ELAPSED_TIME number, ACTUAL_ELAPSED_TIME number, 
            AIR_TIME number, DISTANCE number, CARRIER_DELAY number, 
            WEATHER_DELAY number, NAS_DELAY number, SECURITY_DELAY number,
            LATE_AIRCRAFT_DELAY number
            );''')
cur.executemany('''INSERT INTO flights VALUES 
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?);''', to_db)

# create the airlines table
with open('./hw1/airlines.csv') as airlines_table:
    dr = csv.DictReader(airlines_table, delimiter=',')
    cols = dr.fieldnames
    to_db = [tuple([i[j] for j in cols]) for i in dr]

cur.execute("drop table if exists airlines;")
cur.execute('''CREATE TABLE airlines (
            OP_UNIQUE_CARRIER text,
            FULL_OP_UNIQUE_CARRIER text);''')
cur.executemany('''INSERT INTO airlines VALUES 
                   (?, ?);''', to_db)

# create the airports table
with open('./hw1/airports.csv') as airports_table:
    dr = csv.DictReader(airports_table, delimiter=',')
    cols = dr.fieldnames
    to_db = [tuple([i[j] for j in cols]) for i in dr]

cur.execute("drop table if exists airports;")
cur.execute('''CREATE TABLE airports (
            AIRPORT_ID number,
            FULL_AIRPORT_ID text);''')
cur.executemany('''INSERT INTO airports VALUES 
                   (?, ?);''', to_db)

# create the cancellation table
with open('./hw1/cancellations.csv') as cancellations_table:
    dr = csv.DictReader(cancellations_table, delimiter=',')
    cols = dr.fieldnames
    to_db = [tuple([i[j] for j in cols]) for i in dr]

cur.execute("drop table if exists cancellations;")
cur.execute('''CREATE TABLE cancellations (
            CODE text,
            CODE_DESCRIPTION text);''')
cur.executemany('''INSERT INTO cancellations VALUES 
                   (?, ?);''', to_db)

con.commit()


