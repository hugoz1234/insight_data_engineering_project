        # business_id text,
        # day text,
        # visit_time timestamp,
        # visits int,
        # user_id text,
        # PRIMARY KEY ((business_id, day), visit_time)
import uuid
from datetime import datetime, timedelta
import csv

def produce_csv():
	"""Produce placeholder traffic data. One visit to hardcoded b_id every second for thirty seconds"""
	csv_file = 'front_end_test.csv'
	b_id = 'le-french-tart-brooklyn'
	day = 'October 1'
	user_id = uuid.uuid4()
	time = datetime.now().replace(microsecond=0)
	end_time = time + timedelta(seconds=30)
	with open(csv_file, 'w') as _file:
		while time < end_time:
			row = []
			row.append(b_id)
			row.append(day)
			row.append(str(time))
			row.append(1)
			row.append(user_id)
			w = csv.writer(_file)
			w.writerow(row)
			time = time + timedelta(seconds=1)

if __name__ == '__main__':
	produce_csv()
