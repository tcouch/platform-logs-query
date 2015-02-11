from logs import LegionLogs as logs
from rcops import rcops
from ldapquery import LDAPCon
import datetime as dt
import csv, math


class KPIs:
    def __init__(self,startDate,endDate):
        
        if startDate >= endDate:
            raise ValueError("The start date must be before the end date...")
        

        #convert time to epoch units
        self.startDate = startDate
        self.endDate = endDate
        self.QueryStartEpoch = int((startDate - dt.datetime(1970,1,1)).total_seconds()) # output datetime object properties as epoch
        self.QueryEndEpoch = int((endDate - dt.datetime(1970,1,1)).total_seconds()) # output datetime object properties as epoch
        #print "Start date ", self.startDate, " converted to ", self.QueryStartEpoch
        #print "End date ", self.endDate, " converted to ", self.QueryEndEpoch

        #prepare the logs to be queried
        self.log=logs()
        #prepare ldap connection for user information query
        self.ldap_con=LDAPCon()

    def getActiveUserStats(self):

        #Query to get list of all jobs within time period returning start_time, end_time, cost and owner for
        #core hours calculation.
        query = ("select owner, ((end_time - start_time)*cost)/3600 as core_hours from sgelogs.accounting " + \
                 "where start_time > %s" + \
                 " and start_time <= %s") %(self.QueryStartEpoch, self.QueryEndEpoch)
        query = query + " and owner not regexp '^cours'"    #remove training course accounts
        #remove research computing operations people
        for user in rcops:
            query = query + " and owner != \"%s\"" % (rcops[user])
        query = query + " ; "
        jobList = self.log.query(query)
        
        #generate list of dictionaries containing unique userids and sum of core hours used by running through joblist
        activeUsers = list()
        users = list()  #list used to check whether user has been added yet
        for job in jobList:
            if not job['owner'] in users:
                activeUsers.append({'userid':job['owner'], 'core_hours':job['core_hours']})
                users.append(job['owner'])
            else:
                for i in activeUsers:
                    if i['userid'] == job['owner']:
                        i['core_hours'] += job['core_hours']

        #query ldap for each user and add details for each user
        for user in activeUsers:
            user_details = self.ldap_con.query_user(user['userid'])
            try:
                dept = ''.join(user_details[0][-1].get('department','Department not found'))
                sn = ''.join(user_details[0][-1].get('sn','Surname not found'))
                givenName = ''.join(user_details[0][-1].get('givenName','given name not found'))
                faculty = get_faculty(dept)
                user['surname'] = sn
                user['department'] = dept
                user['faculty'] = faculty
                user['given name'] = givenName
            except:
                user['surname'] = "Not Found"
                user['department'] = "Not Found"
                user['faculty'] = "Not Found"
                user['given name'] = "Not Found"
        return activeUsers

    def getSlowdownStats(self, node="all"):
        # Query slowdown for each job, discounting all but the first jobs in job arrays
        # This query uses complicated substring thingy to get requested run time out of category field as this isn't done already in database :-(
        requested_run_time = "left(substring(category,locate('h_rt=',category)+5),locate(',',substring(category,locate('h_rt=',category)+5))-1)"
        wait_time = "start_time - submission_time"
        slowdown_calculation = "(" + wait_time + "+" + requested_run_time + ")/" + requested_run_time

        # get node selector regular expression
        if node != "all":
            nodeSelector = "and hostname regexp '%s' " % (node2hostnames(node))
        else:
            nodeSelector = ""
        
        query = ("select submission_time, MIN(start_time), owner, " \
                 + slowdown_calculation + " as slowdown " \
                 + "from sgelogs.accounting " \
                 + "where category LIKE '%%h_rt=%%' " \
                 + "and submission_time <= start_time " \
                 + "and start_time > {0} " \
                 + "and start_time <= {1} " \
                 + nodeSelector \
                 + "group by job_number;").format(self.QueryStartEpoch, self.QueryEndEpoch)
        print(query)
        slowdownJoblist = self.log.query(query)
        slowdownList = []
        for job in slowdownJoblist:
            slowdownList.append(job['slowdown'])
        slowdownStats = calculate_stats(slowdownList)
        return slowdownStats

    def getServiceAvailability(self):
        # Query the total service availability (time only - no core count necessary)
        query = ("select count(distinct epochtime)*300 as seconds_out from sysadmin.corecount" \
              + " where cores = 0 and epochtime > %s" \
              + " and epochtime < %s;") % (self.QueryStartEpoch,self.QueryEndEpoch)
        seconds_out=self.log.query(query)[0]["seconds_out"]
        total_seconds_available = int(self.QueryEndEpoch)-int(self.QueryStartEpoch) 
        serviceAvailability = 100.0-(100.0 * float(seconds_out))/float(total_seconds_available)
        return serviceAvailability

    def getUtilisation(self):
        # Query the utilisation of the available service
        # Subtracting 2419200 (four weeks) from start_time for lower threshold in order to ensure that all jobs running during the period are included
        query = ("select sum((if(end_time < {1}, end_time, {1}) - if(start_time > {0}, start_time, {0}))*cost) as total_scheduled_CPU_time " \
                 + "from sgelogs.accounting where start_time <={1} and end_time >={0}").format(self.QueryStartEpoch, self.QueryEndEpoch)
        for user in rcops:
            query = query + " and owner != \"%s\"" % (rcops[user]) 
        query = query + " ; "
        #print query
        total_scheduled_CPU_time = self.log.query(query)[0]["total_scheduled_CPU_time"]
         
        query = "select sum(cores)*300 as total_CPU_time_available from sysadmin.corecount " \
                + "where epochtime > %s and epochtime < %s;" % (self.QueryStartEpoch,self.QueryEndEpoch)
        total_CPU_time_available=self.log.query(query)[0]["total_CPU_time_available"]
        utilisation = 100.0 * float(total_scheduled_CPU_time)/float(total_CPU_time_available)
        return utilisation

    def getCoreAvailability(self):
        query = "select avg(cores/total)*100 as core_availability from sysadmin.corecount " \
                + "where epochtime > %s and epochtime < %s;" % (self.QueryStartEpoch,self.QueryEndEpoch)
        coreAvailability=self.log.query(query)[0]["core_availability"]
        return coreAvailability


def calculate_stats(nums):
    stats = {}
    try:
        stats['mean'] = sum(nums)/len(nums)
        stats['median'] = calculateQuartile(nums,2)
        stats['Q1'] = calculateQuartile(nums,1)
        stats['Q3'] = calculateQuartile(nums,3)
        stats['minimum'] = min(nums)
        stats['maximum'] = max(nums)
        stats['count'] = len(nums)
    except:
        stats['mean'] = 0
        stats['median'] = 0
        stats['Q1'] = 0
        stats['Q3'] = 0
        stats['minimum'] = 0
        stats['maximum'] = 0
        stats['count'] = len(nums)
    return stats


def calculateQuartile(nums,quart):
    #this is how excel calculates quartiles
    nums.sort()
    k = (float(quart)/4)*(len(nums)-1)+1
    f = k - math.floor(k)
    k = int(math.floor(k))
    return nums[k] + (f*(nums[k+1] - nums[k]))


def get_faculty(dept):
    #lookup faculty in orgchart file using department
    orgchart = "orgchart.csv"
    with open(orgchart, 'rb') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if dept == row[0]:
                return row[6]
    return "Faculty not found"


def makeActiveUsersCSV(activeUsers,startDate,endDate):
    keys = ['userid', 'surname', 'given name', 'department', 'faculty', 'core_hours']
    filename = "activeUsers_%s_%s.csv" % (startDate.strftime('%d%m%Y'), endDate.strftime('%d%m%Y'))
    f = open(filename, 'wb')
    dict_writer = csv.DictWriter(f, keys)
    dict_writer.writer.writerow(keys)
    dict_writer.writerows(activeUsers)

def node2hostnames(node):
    nodeDict = {}
    nodeDict['W'] = 'node-[a-j]'
    nodeDict['X'] = 'node-[k-p]'
    nodeDict['Y'] = 'node-[0-1]'
    nodeDict['Z'] = 'node-2'
    nodeDict['V'] = 'node-3'
    nodeDict['U'] = 'node-[5rst]'
    nodeDict['T'] = 'node-6'
    return nodeDict[node]


if __name__=="__main__":

    startDate = dt.datetime(2015,1,1)
    endDate = dt.datetime(2015,2,1)

    kpis = KPIs(startDate,endDate)
    activeUsers = kpis.getActiveUserStats()
    slowdownStats = kpis.getSlowdownStats()

    print ""
    print "In the period from ", startDate, " to ", endDate, " :"
    print ""
    print "Active users: ", len(activeUsers)
    print "Availability: ", kpis.getServiceAvailability()
    print "Utilisation: ", kpis.getUtilisation()
    print "Core Availability: ", kpis.getCoreAvailability()
    #Number of jobs in this case counts arrays as a single job
    print "Number of jobs: ", slowdownStats['count']
    print "Mean Slowdown: ", slowdownStats['mean']
    print "MIN Slowdown: ", slowdownStats['minimum']
    print "Q1 Slowdown: ", slowdownStats['Q1']
    print "Median Slowdown: ", slowdownStats['median']
    print "Q3 Slowdown: ", slowdownStats['Q3']
    print "MAX Slowdown: ", slowdownStats['maximum']

    makeActiveUsersCSV(activeUsers,startDate,endDate)
