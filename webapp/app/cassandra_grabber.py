from collections import Counter

from datetime import datetime, timedelta
import heapq

#FOR every business in NYC
# get everyvisit in the last 30 seconds
# count number visits persecond (skip for now)
# put time series and count in highchart && location in google map

# SURGE_THRESHOLD (total average, [timeseries average])

N_BUSIEST_BUSINESSES = 10

def get_time_series(traffic):
    """return time series representation of traffic data"""
    business_time_series = {} #b_id -> {times: [str], visits: [int]}
    all_times = set()
    aux_hash = {} #b_id --> [(ts, visits)]
    aux_helper = {} # b_id -> set(timestamps for visits)
    for b in traffic:
        b_id = b['business_id']
        ts = b['visit_time']
        all_times.add(ts)
        visits = b['visits']
        if b_id not in aux_hash:
            aux_hash[b_id] = [(ts, visits)]#{'times': [ts], 'visits': [visits]}
        else:
            aux_hash[b_id].append((ts, visits))
        if b_id not in aux_helper:
            aux_helper[b_id] = set([ts])
        else:
            aux_helper[b_id].add(ts)
    for ts in all_times: # adds all timestamps to ts for which visit count is 0
        for b_id in aux_helper:
            if ts not in aux_helper[b_id]:
                aux_hash[b_id].append((ts, 0))

    # print "FINISHED AUX HASH: ", len(aux_hash)
    # print aux_hash
    for b in aux_hash:
        ts_sorted = sorted(aux_hash[b], key=lambda x: datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S"))
        # print ts_sorted
        unzipped = zip(*ts_sorted)
        business_time_series[b] = {'times': list(unzipped[0]), 'visits':list(unzipped[1])}

    return business_time_series

def remove_non_surging_businesses(time_series, expected_traffic_average):
    """If over 100 counts for at least half of the seconds it will be kept"""
    surging_business_time_series = {}
    heap = []
    for b_id in time_series:
        counts = time_series[b_id]['visits']
        heap.append((sum(counts), b_id))
    for _, top_b_id in heapq.nlargest(N_BUSIEST_BUSINESSES, heap):
        surging_business_time_series[top_b_id] = time_series[top_b_id]
    print '***************', surging_business_time_series
    return surging_business_time_series

def recalculate_surge(time_series):
    if len(time_series) == 0:
        return (0, [])
    total_average = 0
    average_time_series = []
    av_time_series_counter = Counter() # time -> counts
    total_businesses = float(len(time_series))
    for b_id in time_series:
        visits = time_series[b_id]['visits']
        time_stamps = time_series[b_id]['times']
        # if visits:
        #     total_average += sum(visits)
        #     average_time_series = [x + y for x, y in zip(average_time_series, visits)]
        if visits:
            for ts_index in range(len(visits)):
                av_time_series_counter[time_stamps[ts_index]] += visits[ts_index]

    for ts in av_time_series_counter:
        av_time_series_counter[ts] = av_time_series_counter[ts]/total_businesses

    #normalize
    total_average = total_average/total_businesses
    sorted_ts_tuples = sorted(av_time_series_counter.items(), key=lambda x: datetime.strptime(x[0], "%Y-%m-%d %H:%M:%S"))
    average_time_series = zip(*sorted_ts_tuples)[1]
    # average_time_series = [x/total_businesses for x in average_time_series]
    return (total_average, average_time_series)


def get_traffic_and_bussinesses(rows):
    """returns traffic as time series(dict of dicts) and businesses (list of strs)"""
    traffic = []
    for row in rows:
        d = {}
        d['business_id'] = row.business_id
        d['day'] = row.day
        d['visit_time'] = str(row.visit_time)
        d['visits'] = row.visits
        traffic.append(d)
    if len(traffic) == 0:
        print "WTFFFFFFFFF"
    all_time_series = get_time_series(traffic)
    surge_metrics = recalculate_surge(all_time_series)
    surging_time_series = remove_non_surging_businesses(all_time_series, surge_metrics[0])

    return surging_time_series, surging_time_series.keys(), surge_metrics


def get_businessdata_query_string(businesses):
    if len(businesses) == 0:
        return ''
    q = 'SELECT * FROM business_data WHERE business_id in ('
    for index, b in enumerate(businesses):
        q += '\'' + b + '\''
        if index != len(businesses) - 1: # last
            q += ', '
        else:
            q += ')'
    return q


def send_business_data_to_dicts(rows):
    """Return list of dicts"""
    businesses = []
    b_ids = [] #TODO delete me
    for row in rows:
        d = {}
        d['business_id'] = row.business_id
        b_ids.append(row.business_id)
        d['name'] = row.name
        d['address'] = row.address
        d['latitude'] = row.latitude
        d['longitude'] = row.longitude
        businesses.append(d)
    # print "THESE ARE THE BUSINESSES I GOT: ", b_ids
    return businesses


def get_data(session):
    """
    > Grabs all business traffic in last 30 seconds.
        TODO: change to 30 seconds, also might be grabbing too much data?
    > Filters out all businesses with no surges (right now based on arbitrary surge, later use historical average)
    > Display Google maps pins of surging businesses
    > Upon clicking on pin, display time series graph
    """
    present = (datetime.utcnow() + timedelta(seconds=1)).replace(microsecond=0)
    past = (present - timedelta(seconds=61)).replace(microsecond=0)
    # print '#####################', past, present

    cql = 'SELECT * FROM latest_traffic_data \
    WHERE visit_time > \'{past}\' ALLOW FILTERING'.format(past=past)
    time_series, businesses, surge_metrics = get_traffic_and_bussinesses(session.execute(cql))
    business_data = {}

    if len(businesses) > 0:
        cql2 = get_businessdata_query_string(businesses)
        business_data = send_business_data_to_dicts(session.execute(cql2))
        
    return business_data, time_series, surge_metrics # dict, dict, tuple(total_avg, avg_timeseries)
