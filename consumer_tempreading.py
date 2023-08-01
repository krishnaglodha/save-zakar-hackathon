"""
This example consumers messages from Memphis and prints them to the console.
"""

import argparse
import asyncio
import json
import os

from memphis import Memphis, MemphisError, MemphisConnectError, MemphisHeaderError
mygeoj = {
  "type": "FeatureCollection",
  "features": []
}
async def main(host, username, password, account_id):
    try:
        memphis = Memphis()
        await memphis.connect(host=host,
                              username=username,
                              password=password,
                              account_id=account_id)
        
        consumer = await memphis.consumer(station_name="zakar-temperature-readings", consumer_name="printing-consumer")

        while True:
            batch = await consumer.fetch()
            if batch is not None:
                for msg in batch:
                    serialized_record = msg.get_data()
                    record = json.loads(serialized_record)
                    print(type(record))
                    feat = {
                        "type": "Feature",
                        "properties": {
                            'day':record['day'],
                            'temperature':record['temperature']
                        },
                        "geometry": {
                            "coordinates": [
                           record['geospatial_x'],
                            record['geospatial_y']
                            ],
                            "type": "Point"
                        }
                        }
                    mygeoj['features'].append(feat)
                    
                json.dumps(mygeoj)

        # Step 3b: Save the dictionary as JSON file
        # Replace 'data.json' with the desired file path
                with open('temperaturereadings.geojson', 'w') as json_file:
                    json.dump(mygeoj, json_file)
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
