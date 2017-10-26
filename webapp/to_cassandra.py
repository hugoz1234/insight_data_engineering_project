""" This is a utility module for formatting data acquired through Yelp Fusion API to CSV format in 
	order to bulk load into Cassandra. 

	Cassandra cql command: COPY <table_name> FROM <file_name>

"""
import json
import csv

def get_flattened_businesses(data_file_name):
	businesses = []
	with open(data_file_name, 'r') as responses:
		for resp in responses:
			data = json.loads(resp)['businesses']
			businesses.extend(data)
	return businesses

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
