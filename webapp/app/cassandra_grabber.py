from datetime import datetime, timedelta

def send_traffic_to_dicts(rows):
    traffic = []
    for row in rows:
        d = {}
        d['business_id'] = row.business_id
        d['day'] = row.day
        d['visit_time'] = row.visit_time
        d['user_id'] = row.user_id
        d['visits'] = row.visits
        traffic.append(d)

    return traffic

#FOR every business in NYC
# get everyvisit in the last 30 seconds
# count number visits persecond (skip for now)
# put timeseries and count in highchart && location in google map

def send_business_data_to_dicts(rows):
    businesses = []
    for row in rows:
        d = {}
        d['business_id'] = row.business_id
        d['name'] = row.name
        d['address'] = row.address
        d['latitude'] = row.latitude
        d['longitude'] = row.longitude
        businesses.append(d)

    return businesses


def format_traffic_to_time_series(traffic):
    business_time_series = {} #b_id -> {times: [str], counts: [int]}
    for b in traffic:
        b_id = b['business_id']
        ts = b['visit_time']
        visits = b['visits']
        if b_id not in business_time_series:
            business_time_series[b_id] = {'times': [ts], 'visits': [visits]}
        else:
            business_time_series[b_id]['times'].append(ts)
            business_time_series[b_id]['visits'].append(visits)
    return business_time_series


def get_data(session):
    past = (datetime.now() - timedelta(seconds=30)).replace(microsecond=0)

    cql = 'SELECT * FROM latest_traffic_data \
    WHERE business_id=\'le-french-tart-brooklyn\' \
    AND visit_time > \'{past}\' ALLOW FILTERING'.format(past=past)
    traffic = send_traffic_to_dicts(session.execute(cql))
    formatted_traffic = format_traffic_to_time_series(traffic)

    cql2 = 'SELECT * FROM business_data \
    WHERE business_id=\'le-french-tart-brooklyn\''
    business_data = send_business_data_to_dicts(session.execute(cql2))

    return (business_data, formatted_traffic)