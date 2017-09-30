import requests
import pprint 
import json

def run():
	access_token = 'UGVdSCSyeZesU-Bn8MF9VQ-r4EdrkumK9QA5tVcSxe7KppukOTuqDZ3_2lXefBxcLEdUz0rIJgjuok31B9zgfvcCq_CP_9pHEpQOx1wX3SYACUKiEXKqUWYjXf3DWXYx'
	url = 'https://api.yelp.com/v3/businesses/search'
	headers = {'Authorization': 'bearer %s' % access_token}
	params = {'location': 'New York',
	          'limit': 50,
	          'offset': 0,
	         }
	total_businesses = 0
	offset = 20
	while offset > 0:
		try:
			resp = requests.get(url=url, params=params, headers=headers)
			with open('nyc_businesses', 'a') as nyc_businesses:
				nyc_businesses.write(json.dumps(resp.json()))
				nyc_businesses.write('\n')
			params['offset'] += 50
			total_businesses += len(resp.json()['businesses'])
		except Exception as e:
			print ('Failure: ', e)
		offset -= 1
	print ('Gathered ', total_businesses, ' businesses!!')
	# pprint.pprint(resp.json()['total'])

if __name__ == '__main__':
	run()