import json
import re

# with open('somefile.txt', 'r') as test:
# 	for line in test:
# 		hopefully_json = json.loads(line)
# 		print type(hopefully_json)
# 		print hopefully_json
	# allthejson = test.read()
	# r = re.split('(\{.*?\})(?= *\{)', allthejson)
	# for obj in r:
	# 	if len(obj) > 0:
	# 		print json.loads(obj)

# with open('nyc_businesses', 'r') as b:
# 	for line in b:
# 		a = json.loads(line)
# 		for business in a['businesses']:
# 			for key in business:
# 				print key
# 			# print business['categories']


# test_data == data source
# test_file == where the data is being written to
# with open('test_data', 'r') as d:
# 	for line in d:
# 		data = line

# aux = 10
# while aux > 0:
# 	with open('test_file', 'a') as f:
# 		f.write(data)
# 		f.write('\n')
# 	aux -= 1

# with open('nyc_businesses', 'r') as b:
# 	count = 0
# 	for resp in b:
# 		jsonobject = json.loads(resp)
# 		count += len(jsonobject['businesses'])
# 		print count

with open('delete_me.txt', 'w') as x:
	x.write(1.0)


		