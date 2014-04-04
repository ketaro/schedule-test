import csv
import redis
import random
import datetime

CSVFILE='schedule_request.csv'
rs = redis.StrictRedis(host='localhost', port=6379, db=0)

# Read in the CSV file into a list of dictionaries
def import_csv():
    r = []
    headers = []
    
    with open(CSVFILE, 'r') as csvfile:
        schedulereader = csv.reader(csvfile)
        for row in schedulereader:
            if headers == []:
                headers = row
            else:
                d = {}
                for i in range(len(headers)):
                    d[headers[i]] = row[i]
                
                r.append(d)
    
    return r


# Create a day with all zeros
def seed_redis(dept_id, room_id):
    key = ":".join([str(dept_id), 'needs'])
    
    # Create example day of needs
    rs.delete(key)
    
    # Push into list a record for each hour on the day
    for h in range(24):
        for b in range(4):
#            n = int(random.random() * 5)
            rs.rpush(key, 0)


def init_dept_need(dept_id, room_id, datekey):
    needkey = get_dept_need_key(dept_id, room_id, datekey)
    
    rs.delete(needkey)
    
    # Push into list a record for each hour on the day
    for h in range(24):
        for b in range(4):
            rs.rpush(needkey, 0)
    

# Get YYYYMMDD from epoch
def get_epoch_index(ts):
    d = datetime.datetime.fromtimestamp(int(ts))
    return d.strftime('%Y%m%d')

def get_dept_need_key(dept_id, room_id, dt):
    return "%d:%d:needs:%s" % (int(dept_id), int(room_id), dt)


# Pass in a datetime, return the list position of the timestamp
def get_epoch_hour_index(epoch):
    d = datetime.datetime.fromtimestamp(int(epoch))
    return get_hour_index(d)
    
def get_hour_index(dt):    
    return int((dt.hour * 4) + (dt.minute / 15))


def increment_needs(dept_id, room_id, start, end, needed):
    ymd      = get_epoch_index(start)
    need_key = get_dept_need_key(dept_id, room_id, ymd)
    
    startidx = get_epoch_hour_index(start)
    duration = ( int(end) - int(start) ) / 60 / 15
    
    print "start %s  duration: %d" % (startidx, duration)
#    print rs.lrange(need_key, 0, -1)

    try:

        # Watch the redis key
        pipe = rs.pipeline()
        pipe.watch(need_key)

        pipe.multi()
        for i in range(startidx, startidx+duration):
            val = int(rs.lindex(need_key, i))
            val += int(needed)
            
            rs.lset(need_key, i, val)
            
            #print i
        
        pipe.execute()
    
    except redis.WatchError:
        print "WATCH ERROR"
    
    finally:
        pipe.reset()

#    print "DONE\n", rs.lrange(need_key, 0, -1)

# Take a schedule request and set it in redis
def set_dept_needs(request):
    print "dept(%s) room(%s) start(%s) ymd(%s)" % (request['dept_id'], request['room_id'], request['start'], get_epoch_index(request['start']) )
    
    # Check if a key already exists for this dept/room
    ymd = get_epoch_index(request['start'])
    need_key = get_dept_need_key(request['dept_id'], request['room_id'], ymd)
    if rs.llen(need_key) <= 0:
        # init key
        init_dept_need(request['dept_id'], request['room_id'], ymd)
    
    # Increment needs in the list, starting at the start and continue to the duration
    increment_needs(request['dept_id'], request['room_id'], request['start'], request['end'], request['request'])


def get_dept_needs(dt, duration):
    # Search all dept key needs for this date, then get the range from the list for the
    #  hour range
    keypattern = '*:*:needs:%s' % dt.strftime('%Y%m%d')
    keys = rs.keys(keypattern)
    
    for key in keys:
        dept_id, room_id, needs, ymd = key.split(':')
        
        start = get_hour_index(dt)
        stop = start + (int(duration/15)-1)
        needs = rs.lrange(key, start, stop)
    
        print "key: %s  needs: %r" % (key, needs)


if __name__ == '__main__':
    data = import_csv()
    
    # Flush Redis
    rs.flushall()
    
    for item in data:
        if item['reqtype'] == 'deptreq':
            set_dept_needs(item)
            
    
    