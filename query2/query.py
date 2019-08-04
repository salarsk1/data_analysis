import sqlite3
con = sqlite3.connect("air.db")
cur = con.cursor()

'''
# 1
Insert the following records into the table flights:
a. (2020, 1, 1, 6, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '') 
b. (2020, 1, 2, 7, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '')
c. (2020, 1, 2, 7, 'TP', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '')
Print the output of the three queries individually, by selecting the row corresponding to 
YEAR = 2020. You should see after each insert that more rows have been added.
'''

value = [2020, 1, 1, 6, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
cur.execute('''INSERT INTO flights values
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?);''',value)
con.commit()

cur.execute('''select * from flights''')

rows = cur.fetchall()
print('QUESTION 1')
print(rows[-1])


value = [2020, 1, 2, 7, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
cur.execute('''INSERT INTO flights values
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?);''',value)
con.commit()

cur.execute('''select * from flights''')

rows = cur.fetchall()
for row in rows[-2:]:
    print(row)

value = [2020, 1, 2, 7, 'TP', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
cur.execute('''INSERT INTO flights values
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,
                    ?,?,?,?,?,?,?,?,?,?,?,?,?);''',value)
con.commit()

cur.execute('''select * from flights''')

rows = cur.fetchall()
for row in rows[-3:]:
    print(row)

# 2 Update the OP_UNIQUE_CARRIER of the table flights to be 'CS' for YEAR = 2020. 
#   Then print all flights where YEAR = 2020 ​(Make sure you use the commit() function to store the record in the database. )
cur.execute('''UPDATE flights
               SET OP_UNIQUE_CARRIER = "CS"
               WHERE YEAR = 2020''' )
con.commit()

cur.execute('''SELECT * 
               FROM flights 
               WHERE year = 2020''')

rows = cur.fetchall()
print('QUESTION 2')
for row in rows:
    print(row)

# 3 Delete the flight that has the OP_UNIQUE_CARRIER of 'CS'. Then print all flights where YEAR = 2020
cur.execute('''DELETE 
               FROM flights 
               WHERE OP_UNIQUE_CARRIER = "CS";''')

con.commit()

cur.execute('''SELECT * 
               FROM flights 
               WHERE year = 2020''')
rows = cur.fetchall()
print('QUESTION 3')
for row in rows:
    print(row)

'''
# 4 Select the OP_CARRIER_FL_NUM, ORIGIN_AIRPORT_ID, airports.FULL_AIRPORT_ID, 
    and DISTANCE of flights that have trips with more than 4000 miles ​
    (tables = flights + airports, columns to join = ORIGIN_AIRPORT_ID, AIRPORT_ID, columns to match = DISTANCE). 
    Use an INNER JOIN. To only get the unique names, use the keyword DISTINCT (i.e, SELECT DISTINCT name).
'''

cur.execute('''SELECT DISTINCT F.OP_CARRIER_FL_NUM, F.ORIGIN_AIRPORT_ID, airports.FULL_AIRPORT_ID, F.DISTANCE
               from flights F
               inner join airports on airports.AIRPORT_ID = F.ORIGIN_AIRPORT_ID
               where F.DISTANCE > 4000
               order by F.ORIGIN_AIRPORT_ID''')
rows = cur.fetchall()
print('QUESTION 4')
for row in rows:
    print(row)


'''
# 5 Select all DISTINCT airport names (FULL_AIRPORT_ID) from the database where the airline 
    “Delta Air Lines Inc.” has as an Origin Airport (ORIGIN_AIRPORT_ID) .
'''
cur.execute('''SELECT distinct AP.FULL_AIRPORT_ID
               FROM flights F, airports AP
               where F.OP_UNIQUE_CARRIER = "DL" and F.ORIGIN_AIRPORT_ID = AP.AIRPORT_ID''')
rows = cur.fetchall()
print('QUESTION 5')
for row in rows:
    print(row)


'''
# 6 Select all DISTINCT carrier names (FULL_OP_UNIQUE_CARRIER) from the database, 
    where flights in originating from “New York, NY: LaGuardia” were cancelled due to “Weather”.
'''
cur.execute('''SELECT AL.FULL_OP_UNIQUE_CARRIER
               from airlines AL, (SELECT DISTINCT F.OP_UNIQUE_CARRIER
                                  FROM flights F, airlines AL, airports AP, (select * 
                                                                             from cancellations
                                                                             where CODE_DESCRIPTION = "Weather") as CD
                                  where AP.FULL_AIRPORT_ID="New York, NY: LaGuardia" and F.ORIGIN_AIRPORT_ID = AP.AIRPORT_ID and F.CANCELLATION_CODE = CD.CODE) as NS
                where NS.OP_UNIQUE_CARRIER = AL.OP_UNIQUE_CARRIER''')

rows = cur.fetchall()
print('QUESTION 6')
for row in rows:
    print(row)

# 7 Drop the airlines table from the database. Print the names of all the remaining tables in the database.
print('QUESTION 7')
cur.execute('''DROP table if exists airlines;''')
con.commit()
table_names = con.execute("SELECT name FROM sqlite_master WHERE type='table';")
for name in table_names:
    print(name)
