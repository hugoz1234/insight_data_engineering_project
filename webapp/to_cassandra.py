""" This is a utility module for formatting data acquired through Yelp Fusion API to CSV format in 
	order to bulk load into Cassandra. 

	Cassandra cql command: COPY <table_name> FROM <file_name>

"""
import json
import csv


# distance 2440.94343144
# is_closed False
# rating 5.0
# name Coffee Project New York
# transactions []
# url https://www.yelp.com/biz/coffee-project-new-york-new-york?adjust_creative=19jspXUrNQpBfi9cKzKdPg&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=19jspXUrNQpBfi9cKzKdPg
# price $$
# coordinates {u'latitude': 40.72699, u'longitude': -73.98922}
# phone +12122287888
# image_url https://s3-media2.fl.yelpcdn.com/bphoto/qCeQT_p_uNalc9Q1WoT2ag/o.jpg
# location {u'city': u'New York', u'display_address': [u'239 E 5th St', u'New York, NY 10003'], u'country': u'US', u'address2': None, u'address3': u'', u'state': u'NY', u'address1': u'239 E 5th St', u'zip_code': u'10003'}
# display_phone (212) 228-7888
# review_count 449
# id coffee-project-new-york-new-york
# categories [{u'alias': u'coffee', u'title': u'Coffee & Tea'}]

# distance 490.551004287
# is_closed False
# rating 4.5
# name Brooklyn Bridge Park
# transactions []
# url https://www.yelp.com/biz/brooklyn-bridge-park-brooklyn-3?adjust_creative=19jspXUrNQpBfi9cKzKdPg&utm_campaign=yelp_api_v3&utm_medium=api_v3_business_search&utm_source=19jspXUrNQpBfi9cKzKdPg
# coordinates {u'latitude': 40.7016262778116, u'longitude': -73.9972114562988}
# phone +17182229939
# image_url https://s3-media3.fl.yelpcdn.com/bphoto/vtdWyO6PvxhUGqGAzIQvmw/o.jpg
# location {u'city': u'Brooklyn', u'display_address': [u'334 Furman St', u'Brooklyn, NY 11201'], u'country': u'US', u'address2': None, u'address3': u'', u'state': u'NY', u'address1': u'334 Furman St', u'zip_code': u'11201'}
# display_phone (718) 222-9939
# review_count 449
# id brooklyn-bridge-park-brooklyn-3
# categories [{u'alias': u'parks', u'title': u'Parks'}]

def get_flattened_businesses(data_file_name):
	businesses = []
	with open(data_file_name, 'r') as responses:
		for resp in responses:
			data = json.loads(resp)['businesses']
			businesses.extend(data)
	return businesses

# CREATE TABLE yelp_data.business_data (
#     business_id text PRIMARY KEY,
#     name text,
#     address text,
#     city text,
#     state text,
#     postal_code text,
#     latitude float,
#     longitude float,
#     stars float,
#     review_count int,
#     is_open boolean,
#     categories text, #switch to semicolon separated vals, inschema just text

def format_attributes(attributes_list):
	"""attributes_list: list of dictionaries where dict values are business attributes"""
	str_version = ""
	for index, attribute_dict in enumerate(attributes_list):
		for key, attribute in attribute_dict.iteritems():
			if ';' in attribute:
				attribute = attribute.replace(';', ' ')
			str_version += attribute + ';'
	return str_version[:len(str_version)-1] # remove last semicolon

def format_row(row):
	"""use utf8 encoding and remove commas"""
	for index, value in enumerate(row):
		if isinstance(value, basestring):
			if ',' in value:
				value = value.replace(',', ' ')
			row[index] = value.encode('utf-8')
	return row

def write_business_data_csv(data, destination_file):
	with open(destination_file, 'w') as csv_file:
		for business in data:
			row = []
			row.append(business['id']) 
			row.append(business['name']) 
			row.append(''.join(business['location']['display_address'])) 
			row.append(business['location']['city']) 
			row.append(business['location']['state'])
			row.append(business['location']['zip_code'])
			row.append(business['coordinates']['latitude'])
			row.append(business['coordinates']['longitude'])
			row.append(business['rating'])
			row.append(business['review_count'])
			row.append(not business['is_closed'])
			row.append(format_attributes(business['categories']))
			try:
				w = csv.writer(csv_file)
				w.writerow(format_row(row))
			except UnicodeEncodeError as e:
				print ('Encoding error detected: ', e)

def fix_types(destination_file):
	lines = []
	with open(destination_file, 'r') as data:
		for line in data:
			lines.append(line)
		print type(line), '******'
	with open(destination_file, 'w') as data:
		for line in lines:
			values = line.split(',')
			values[6] = float(values[6])
			values[7] = float(values[7])
			values[8] = float(values[8])
			values[9] = int(values[9])
			data.write(','.join(values))

if __name__ == '__main__':
	b_file = 'nyc_businesses'
	destination_file = 'nyc_businesses.csv'
	businesses = get_flattened_businesses(b_file)
	write_business_data_csv(businesses, destination_file)
	fix_types(destination_file)
