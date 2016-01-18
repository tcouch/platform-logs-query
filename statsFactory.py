from logsConnector import logsConnection as logs
from rcops import rcops
from nodeDict import nodeDict
from platform2database import platform2database
import abc
import datetime as dt
import math
import sys


class Statistic(object):
    __metaclass__ = abc.ABCMeta

    @classmethod
    def factory(cls, checkStr):
        return checkStr == cls.__name__

    def __init__(self,*args,**kwargs):

        startDate = kwargs["startDate"]
        endDate = kwargs["endDate"]
        
        if startDate >= endDate:
            raise ValueError("The start date must be before the end date...")

        if "node" in kwargs:
            self.nodeClass = kwargs["node"]
        else:
            self.nodeClass = "all"
        
        #default to latest legion logs database if none specified
        if "db" in kwargs:
            self.db = kwargs["db"]
        else:
            self.db = "sgelogs2"
        
        #convert dates to unix timestamp for database queries
        self.startEpoch = int((startDate - dt.datetime(1970,1,1)).total_seconds())
        self.endEpoch = int((endDate - dt.datetime(1970,1,1)).total_seconds())

        self.dbConnection = logs()

        self.constructQuery()
        
    @abc.abstractmethod
    def constructQuery(self):
        """Method for constructing the query string
        used for implementing statistic in the database.

        Query string should return a single value labelled 'result'
        """

    def getResult(self):
        return self.dbConnection.query(self.constructQuery())[0]["result"]

class ServiceAvailability(Statistic):
    def constructQuery(self):
        queryString = "select (100 - 100*(count(distinct epochtime)*300)" \
            + "/({1}-{0})) as result" \
            + " from {db}.core_count" \
            + " where cores = 0 and epochtime > {0}" \
            + " and epochtime < {1};"
        queryString = queryString.format(self.startEpoch,self.endEpoch,db=self.db)
        return queryString

class CoreAvailability(Statistic):
    def constructQuery(self):
        queryString = "select avg(cores/total)*100 as result" \
            + " from {db}.core_count" \
            + " where epochtime > {0} and epochtime < {1};"
        queryString = queryString.format(self.startEpoch,self.endEpoch,db=self.db)
        return queryString
        
class Utilisation(Statistic):
    def constructQuery(self):
        queryString = "select(select sum((if(end_time < {1}, end_time, {1})" \
            + "- if(start_time > {0}, start_time, {0}))*cost)" \
            + " from {db}.accounting where start_time <={1}" \
            + " and end_time >={0}" \
            + " and end_time > start_time"
        #remove research computing operations people
        for user in rcops:
            queryString = queryString + " and owner != \"%s\"" % (rcops[user])
        queryString = queryString + ")*100/"
        queryString = queryString + "(select sum(cores)*300" \
                + " from {db}.core_count" \
                + " where epochtime > {0} and epochtime < {1}) as result;"
        queryString = queryString.format(self.startEpoch,self.endEpoch,db=self.db)
        return queryString

class ActiveUsers(Statistic):
    def constructQuery(self):
        queryString = "select count(distinct(owner)) as result" \
                 + " from {db}.accounting" \
                 + " where start_time > {0}" \
                 + " and start_time <= {1}" \
                 + " and end_time > start_time"
        queryString = queryString + " and owner not regexp '^cours'"    #remove training course accounts
        #remove research computing operations people
        for user in rcops:
            queryString = queryString + " and owner != \"%s\"" % (rcops[user])
        queryString = queryString + " ; "
        queryString = queryString.format(self.startEpoch,self.endEpoch,db=self.db)
        return queryString

class Slowdown(Statistic):
    def constructQuery(self):
        requested_run_time = "left(substring(category,locate('h_rt=',category)+5)" \
                             + ",locate(',',substring(category,locate" \
                             + "('h_rt=',category)+5))-1)"
        wait_time = "start_time - submission_time"
        slowdown_calculation = "(" + wait_time + "+" + requested_run_time + ")/" \
                               + requested_run_time

        # get node selector regular expression
        if self.nodeClass != "all":
            nodeSelector = "and hostname regexp '%s' " % (node2hostnames(self.nodeClass))
        else:
            nodeSelector = ""

        queryString = "select MIN(start_time), " + slowdown_calculation + " as result " \
                 + "from {db}.accounting " \
                 + "where category LIKE '%%h_rt=%%' " \
                 + "and submission_time <= start_time " \
                 + "and start_time > {0} " \
                 + "and start_time <= {1} " \
                 + "and end_time > start_time " \
                 + nodeSelector \
                 + "group by job_number;"
        queryString = queryString.format(self.startEpoch,self.endEpoch,db=self.db)
        return queryString

    def getResult(self):
        raw = self.dbConnection.query(self.constructQuery())
        slowdownList = []
        for row in raw:
            slowdownList.append(row['result'])
        slowdownStats = calculate_stats(slowdownList)
        return slowdownStats
        

def statFactory(statRequest,*args,**kwargs):
    for cls in Statistic.__subclasses__():
        if cls.factory(statRequest):
            return cls(*args,**kwargs)

def node2hostnames(node):
    return nodeDict[node]

def calculate_stats(nums):
    stats = {}
    stats['mean'] = sum(nums)/len(nums)
    stats['median'] = calculateQuartile(nums,2)
    stats['Q1'] = calculateQuartile(nums,1)
    stats['Q3'] = calculateQuartile(nums,3)
    stats['minimum'] = min(nums)
    stats['maximum'] = max(nums)
    stats['count'] = len(nums)
    return stats

def calculateQuartile(nums,quart):
    #this is how excel calculates quartiles
    nums.sort()
    k = (float(quart)/4)*(len(nums)-1)+1
    f = k - math.floor(k)
    k = int(math.floor(k))
    return nums[k] + (f*(nums[k+1] - nums[k]))

class NullWriter(object):
    def write(self, arg):
        pass



def main():    
    startDate = dt.datetime(2015,12,1)
    endDate = dt.datetime(2016,1,1)

##    nullwrite = NullWriter()
##    oldstdout = sys.stdout
##    sys.stdout = nullwrite

    kwargs = {
            "startDate": startDate,
            "endDate": endDate,
            "db": "sgelogs2",
            "node": "all"
            }

    f = statFactory("ServiceAvailability", **kwargs).getResult

    g = statFactory("CoreAvailability", **kwargs).getResult

    h = statFactory("Utilisation", **kwargs).getResult

    i = statFactory("ActiveUsers", **kwargs).getResult

    j = statFactory("Slowdown", **kwargs).getResult

##    sys.stdout = oldstdout

    print f.__self__.__class__.__name__, ":", f()
    print g.__self__.__class__.__name__, ":", g()
    print h.__self__.__class__.__name__, ":", h()
    print i.__self__.__class__.__name__, ":", i()
    print j.__self__.__class__.__name__, ":"
    slowdown = j()
    for key, value in slowdown.iteritems():
        print "\t{0}: {1}".format(key, value)

if __name__ =="__main__":
    main()
