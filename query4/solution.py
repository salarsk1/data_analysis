import sqlite3
import csv
con = sqlite3.connect("air.db")
cur = con.cursor()
cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS flights_ind ON flights(ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, MONTH, DAY_OF_MONTH, OP_CARRIER_FL_NUM, CRS_DEP_TIME, DEP_TIME);''')
cur.execute('''SELECT DEP_DELAY, ARR_DELAY \
               FROM flights \
               inner join airports on  flights.ORIGIN_AIRPORT_ID = airports.AIRPORT_ID \
               inner join airlines on  flights.OP_UNIQUE_CARRIER  = airlines.OP_UNIQUE_CARRIER \
               where airports.FULL_AIRPORT_ID = "Chicago, IL: Chicago O'Hare International" and airlines.FULL_OP_UNIQUE_CARRIER = "Delta Air Lines Inc."
               ORDER BY DEP_DELAY ASC, ARR_DELAY ASC; ''')


p3 = cur.fetchall()
with open('q3.txt', 'w') as f:
    for row in p3:
        if row[0] != '' and row[1]!='':
            f.write(f"{row[0]}.0,{row[1]}.0\n")
        #     f.write(', '.join(i for i in row) + '\n')

cur.execute('''SELECT DAY_OF_WEEK, count(*)
               FROM flights 
               GROUP BY DAY_OF_WEEK''')

p4 = cur.fetchall()
with open('q4.txt', 'w') as f:
    for row in p4:
        f.write(', '.join(str(i) for i in row) + '\n')


cur.execute('''SELECT MONTH, DAY_OF_MONTH, count(*)
               from flights
               inner join airports on flights.DEST_AIRPORT_ID = airports.AIRPORT_ID
               where airports.FULL_AIRPORT_ID = "Honolulu, HI: Daniel K Inouye International"
               GROUP BY MONTH, DAY_OF_MONTH;''')

p5 = cur.fetchall()
with open('q5.txt', 'w') as f:
    for row in p5:
        f.write(f"{row[0]},{row[1]},{row[2]}\n")

cur.execute('''SELECT MONTH, DAY_OF_MONTH, SUM(WEATHER_DELAY)
               from flights
               GROUP BY MONTH, DAY_OF_MONTH ''')

p6 = cur.fetchall()
with open('q6.txt', 'w') as f:
    for row in p6:
        f.write(f"{row[0]},{row[1]},{row[2]}\n")