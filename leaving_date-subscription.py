from datetime import datetime, timezone
from pymongo import MongoClient

# creation of MongoClient

db_url = "mongodb://localhost:27017/"
client = MongoClient(db_url)

database = client["HealthSure"]
file_path = "/Users/Shubham/Downloads/27th-leaving_date.csv"

subscription_collection = database["subscriptions"]

current_time_stamp = int(datetime.now().timestamp())


def get_time_stamp(date_string):
    date_object = datetime.strptime(date_string, "%d/%m/%Y")
    return date_object.replace(tzinfo=timezone.utc).timestamp()


f = open(file_path, 'r')
date_map = {}
for line in f.readlines()[1:]:
    oshs_id, new_date_string= line.split(",")
    oshs_id = oshs_id.strip()
    new_date_string = new_date_string.strip()

    if new_date_string not in date_map:
        date_map[new_date_string] = []
    date_map[new_date_string].append(oshs_id)

for new_date, oshs_ids in date_map.items():
    print(new_date, len(oshs_ids))
    new_date_timestamp = int(get_time_stamp(new_date))
    updated = subscription_collection.update_many({"_id": {'$in': oshs_ids}, 'active': False},
                                                  [
                                                      {
                                                          '$set': {'leaving_date': new_date_timestamp,
                                                                   'updated_at': current_time_stamp}
                                                      }
                                                  ]
                                                  )
    print(updated.raw_result)
