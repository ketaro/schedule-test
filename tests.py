import unittest
import schedule
import datetime

class ScheduleTests(unittest.TestCase):
    def test_tests(self):
        self.assertTrue(1 == 1)


    def test_get_epoch_hour_index(self):
        dt = datetime.datetime(2014, 4, 1, 0, 10, 0, 0)
        mins = [0, 15, 30, 45]
        
        for h in range(23):
            dt = dt.replace(hour=h)
            
            for i in range(len(mins)):
                dt = dt.replace(minute=mins[i])
                self.assertEqual(schedule.get_hour_index(dt), h*4 + i)


    def test_00_flush_redis(self):
        schedule.rs.flushall()
        
        keys = schedule.rs.keys()
        
        self.assertEqual(keys, [])
        
    
    def test_01_import_csv(self):
        result = schedule.import_csv()
        
        self.assertTrue(len(result) > 100)
        self.assertTrue('reqtype' in result[0].keys())
        self.assertTrue('disposition' in result[0].keys())
        self.assertTrue('dept_id' in result[0].keys())
        
        # Import data into redis
        for item in result:
            if item['reqtype'] == 'deptreq':
                schedule.set_dept_needs(item)
        
        # Make sure a key got set
        self.assertTrue(schedule.rs.llen('14:0:needs:20130809') > 50)
        
    
    def test_02_find_dept_needs(self):
        d = datetime.datetime(2013, 8, 10, 7, 0)
        
        needs = schedule.get_dept_needs(d, 60)
        
        self.assertTrue(needs > 0)


    def Xtest_seed(self):
        schedule.seed_redis(1)
        schedule.seed_redis(2)
        schedule.seed_redis(3)
        schedule.seed_redis(4)
        schedule.seed_redis(5)
        
        self.assertEqual(schedule.rs.llen('1:needs'), 96)
        self.assertEqual(schedule.rs.llen('2:needs'), 96)
        self.assertEqual(schedule.rs.llen('3:needs'), 96)
        self.assertEqual(schedule.rs.llen('4:needs'), 96)
        self.assertEqual(schedule.rs.llen('5:needs'), 96)
    
    

if __name__ == '__main__':
    unittest.main()