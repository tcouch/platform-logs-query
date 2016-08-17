from logsConnector import logsConnection as logs
from ldapquery import LDAPCon
from rcops import rcops
from platform2database import platform2database
import pickle, csv
import datetime as dt


class jobHistory(object):


    def __init__(self,*args,**kwargs):

        self.startDate = kwargs["startDate"]
        self.endDate = kwargs["endDate"]
        
        if self.startDate >= self.endDate:
            raise ValueError("The start date must be before the end date...")
        
        #convert dates to unix timestamp for database queries
        self.startEpoch = int((self.startDate - dt.datetime(1970,1,1)).total_seconds())
        self.endEpoch = int((self.endDate - dt.datetime(1970,1,1)).total_seconds())

        #default to latest legion logs database if no platform specified
        if "platform" in kwargs:
            self.platform = kwargs["platform"]
            self.db = platform2database[kwargs["platform"]]
        else:
            self.platform = "Legion"
            self.db = "sgelogs2"

        #List of users to search for
        if "usernames" in kwargs:
            self.usernames = kwargs["usernames"]
        else:
            self.usernames = []

        #prepare database connection
        self.dbConnection = logs()

        #prepare ldap connection for user information query
        self.ldap_con=LDAPCon()

        self.query = self.constructQuery()


    def constructQuery(self):
        queryString = "select owner, ((end_time - start_time)*cost)/3600 as core_hours" \
                + " from {db}.accounting" \
                + " where start_time > {0}" \
                + " and start_time <= {1}" \
		+ " and end_time > start_time"
        #add list of users to search for if present
        if self.usernames:
            for username in self.usernames:
                queryString = queryString + " and owner = '%s'" % (username)
        #if no users specified just exclude course accounts and rcops
        else:
            #remove training course accounts
            queryString = queryString + " and owner not regexp '^cours'"
            #remove research computing operations people
            for user in rcops:
                queryString = queryString + " and owner != '%s'" % (rcops[user])
        #Finish query
        queryString = queryString + " ; "
        queryString = queryString.format(self.startEpoch, self.endEpoch, db=self.db)
        print(queryString)
        return queryString


    def processResult(self):
        """generate list of dictionaries containing unique userids
        and sum of core hours used by running through joblist. Then query
        ldap to find name, department etc."""
        jobList = self.dbConnection.query(self.query)
        userSummary = list()
        users = list()  #list used to check whether user has been added yet
        for job in jobList:
            if not job['owner'] in users:
                with open('userdata.pkl','rb') as ldapDataFile:
                    ldapData = pickle.load(ldapDataFile)
                if filter(lambda ldapUserData: ldapUserData['userid'] == job['owner'], ldapData):
                    ldapcreds = filter(lambda ldapUserData: ldapUserData['userid'] == job['owner'], ldapData)[0]
                    dept = ldapcreds['department']
                    faculty = ldapcreds['faculty']
                    sn = ldapcreds['surname']
                    givenName = ldapcreds['given name']
                else:
                    ldapcreds = self.ldap_con.query_user(job['owner'])
                    try:
                        dept = ''.join(ldapcreds[0][-1].get('department','Department not found'))
                        sn = ''.join(ldapcreds[0][-1].get('sn','Surname not found'))
                        givenName = ''.join(ldapcreds[0][-1].get('givenName','given name not found'))
                        faculty = self.get_faculty(dept)
                    except:
                        sn = "Not Found"
                        dept = "Not Found"
                        givenName = "Not Found"
                        faculty = "Not Found"
                    ldapData.append({'userid':job['owner'],
                                     'department':dept,
                                     'faculty':faculty,
                                     'surname':sn,
                                     'given name':givenName})
                userSummary.append({'userid':job['owner'],
                                    'core_hours':job['core_hours'],
                                    'surname':sn,
                                    'given name':givenName,
                                    'department':dept,
                                    'faculty':faculty})
                users.append(job['owner'])
                with open('userdata.pkl', 'wb') as ldapDataFile:
                    pickle.dump(ldapData,ldapDataFile)
            else:
                filter(lambda x: x['userid'] == job['owner'], userSummary)[0]['core_hours'] += job['core_hours']
        return userSummary


    def printResults(self):
        userSummary = self.processResult()
        print("Platform: {platform}".format(platform=self.platform))
        print("From: {0} to {1}".format(self.startDate,self.endDate))
        print("UserID \t Given Name \t Surname \t Department \t Core Hours")
        for user in userSummary:
            dataRow = "{0} \t {1} \t {2} \t {3} \t {4}"
            print(dataRow.format(user["userid"], user["given name"],
                                 user["surname"], user["department"], user["core_hours"]))
        print("Done")


    def makeResultsCSV(self):
        userSummary = self.processResult()
        keys = ['userid', 'surname', 'given name', 'department', 'faculty', 'core_hours']
        filename = "activeUsers_%s_%s_%s.csv" % (self.platform,self.startDate.strftime('%d%m%Y'), self.endDate.strftime('%d%m%Y'))
        f = open(filename, 'wb')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        dict_writer.writerows(userSummary)


    def get_faculty(self,dept):
    #lookup faculty in orgchart file using department
        orgchart = "orgchart.csv"
        with open(orgchart, 'rb') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                if dept == row[1]:
                    return row[7]
        return "Faculty not found"
        

def main():
    startDate = dt.datetime(2016,7,1)
    endDate = dt.datetime(2016,8,1)    

    kwargs = {
        "startDate": startDate,
        "endDate": endDate,
#        "platform": "Grace",
#        "usernames": ["cceatco"]
        }

    jobHistory(**kwargs).makeResultsCSV()

        
if __name__ =="__main__":
    main()



