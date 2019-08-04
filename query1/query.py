import sqlite3
con = sqlite3.connect("air.db")
cur = con.cursor()

# 1 Select all from the cancellations table
cur.execute("select * from cancellations;")
rows = cur.fetchall()
for row in rows:
    print(row)

# 2 Select the first 10 rows of FULL_OP_UNIQUE_CARRIER column from the airlines table
cur.execute('''select FULL_OP_UNIQUE_CARRIER from airlines 
               limit 10;''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 3 Select the first 5 rows from the flights table where the OP_UNIQUE_CARRIER = AA
cur.execute('''select * from flights where OP_UNIQUE_CARRIER ="AA" 
               limit 5;''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 4 Select the first 5 rows from the flights table where the OP_UNIQUE_CARRIER = AA, MONTH = 3
cur.execute('''select * from flights 
               where OP_UNIQUE_CARRIER ="AA" and MONTH=1     
               limit 5;''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 5 Select the row from the alrines table where the FULL_OP_UNIQUE_CARRIER = Titan Airways
cur.execute('''select * from airlines 
               where FULL_OP_UNIQUE_CARRIER="Titan Airways"; ''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 6 Select the longest flights to each unique destination and order it in decreasing order of distance. Select the ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, and MAX DISTANCE
cur.execute('''select f.ORIGIN_AIRPORT_ID, f.DEST_AIRPORT_ID, max(DISTANCE) as maximum
                from flights f 
                group by f.DEST_AIRPORT_ID
                order by maximum DESC, f.ORIGIN_AIRPORT_ID ASC, f.DEST_AIRPORT_ID ASC;''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 7 Select the destinations that have less than 10 arrivals. Select the DEST_AIRPORT_ID (Hint: You will need to group the destinations.)
cur.execute('''select DEST_AIRPORT_ID
               from flights
               group by DEST_AIRPORT_ID
               having COUNT(DEST_AIRPORT_ID) < 10;''')
rows = cur.fetchall()
for row in rows:
    print(row)

# 8 Select all of the distinct DAY_OF_MONTH and order them in ascending order.
cur.execute('''select distinct DAY_OF_MONTH
               from flights
               order by DAY_OF_MONTH;''')
rows = cur.fetchall()
for row in rows:
    print(row)
