"""
This example consumers messages from Memphis and prints them to the console.
"""

import argparse
import asyncio
import json
import os
import psycopg2

from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError
supabase_url = "db.kvulzxaujxmffjfnhstj.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt2dWx6eGF1anhtZmZqZm5oc3RqIiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTA2MzM2MTAsImV4cCI6MjAwNjIwOTYxMH0.9TwkwD5t8Rw5uuMR71lnBBtucJ_ggknfn6BP-IJiiOw"

db_pwd = 'saveZakar@123'
def insert_temperature_reading(event_day,notification_day,  latitude, longitude):
    try:
        # Connect to the Supabase database
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password=db_pwd,
            host=supabase_url,
            port="5432",  # Default PostgreSQL port
            options=f"-c search_path=public",
        )

        # Create a cursor to execute queries
        cursor = connection.cursor()

        # Prepare the data
        data = {
            "event_day": event_day,
            "notification_day": notification_day,
            "latitude": latitude,
            "longitude": longitude,
        }

        # Create the SQL query with PostGIS function ST_MakePoint
        insert_query = "INSERT INTO firealerts (event_day, notification_day , geometry) VALUES (%(event_day)s, %(notification_day)s, ST_MakePoint(%(longitude)s, %(latitude)s));"

        # Execute the query
        cursor.execute(insert_query, data)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        print(f"Data inserted successfully. for {event_day} and temp {notification_day}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")

async def main(host, username, password, account_id):
    try:
        memphis = Memphis()
        await memphis.connect(host=host,
                              username=username,
                              password=password,
                              account_id=account_id)
        
        consumer = await memphis.consumer(station_name="zakar-fire-alerts", consumer_name="printing-consumer")

        while True:
            batch = await consumer.fetch()
            if batch is not None:
                for msg in batch:
                    serialized_record = msg.get_data()
                    record = json.loads(serialized_record)
                    print(type(record))
                    insert_temperature_reading(record['event_day'],record['notification_day'],  record['geospatial_y'], record['geospatial_x'])
             
    except (MemphisError, MemphisConnectError) as e:
        print(e)

    finally:
        await memphis.close()

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--host",
                        type=str,
                        required=True,
                        help="Memphis broker host")

    parser.add_argument("--username",
                        type=str,
                        required=True,
                        help="Memphis username")

    parser.add_argument("--password",
                        type=str,
                        required=True,
                        help="Memphis password")

    parser.add_argument("--account-id",
                        type=int,
                        required=True,
                        help="Memphis account ID")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    asyncio.run(main(args.host, args.username, args.password, args.account_id))
