import uuid
import mysql.connector
import datetime
import time

# Establish a connection to the MySQL server
cnx = mysql.connector.connect(user='root', password='7066',
                              host='localhost', database='kafka_demo')

# Create a cursor object to execute queries
cursor = cnx.cursor()

# Insert UUIDs and timestamps indefinitely
while True:
    # Generate a UUID and timestamp
    uuid_value = uuid.uuid4()
    timestamp = datetime.datetime.now()
    
    # Insert the UUID and timestamp into a table called "random_uuids"
    query = "INSERT INTO DAILY_LOAD(A,B,C,D,E,CREATED_AT,UPDATED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (str(uuid_value),str(uuid_value),str(uuid_value),str(uuid_value),str(uuid_value),timestamp, timestamp))
    
    # Commit the changes to the database
    cnx.commit()
    
    # Wait for 1 second before inserting the next UUID and timestamp
    time.sleep(1)

# Close the cursor and connection
cursor.close()
cnx.close()