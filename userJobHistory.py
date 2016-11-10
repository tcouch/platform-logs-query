#!/usr/bin/env python3

from logsConnector import logsConnection as logs
from ldap_lookup import Connection as ldapConnection
from ldap_lookup import Query as ldapQuery
from rcops import rcops
from platform2database import platform2database
from sys import argv
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
        self.ldapConn = ldapConnection()

        self.query = self.constructQuery()


    def constructQuery(self):
        queryString = "select owner, ((end_time - start_time)*cost)/3600 as core_hours" \
                + " from {db}.accounting" \
                + " where start_time > {0}" \
                + " and start_time <= {1}" \
		+ " and end_time > start_time"
        #add list of users to search for if present
        if self.usernames:
            queryString += " and ("
            for username in self.usernames:
                queryString = queryString + " owner = '%s' or" % (username)
            queryString = queryString[:-2] + ")" #remove last or and add closing bracket
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
        fields = ['department', 'sn', 'givenName']
        userSummary = list()
        users = list()  #list used to check whether user has been added yet
        for job in jobList:
            if job['owner'] not in users:
                ldapcreds = ldapQuery(job['owner'], self.ldapConn, fields).get_result()
                dept = ldapcreds.get('department','Department not found')
                sn = ldapcreds.get('sn','Surname not found')
                givenName = ldapcreds.get('givenName','given name not found')
                faculty = self.get_faculty(dept)
                userSummary.append({'userid':job['owner'],
                                    'core_hours':job['core_hours'],
                                    'surname':sn,
                                    'given name':givenName,
                                    'department':dept,
                                    'faculty':faculty})
                users.append(job['owner'])
            else:
                for user in userSummary:
                    if user['userid'] == job['owner']: user['core_hours'] += job['core_hours']
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
        f = open(filename, 'w')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        dict_writer.writerows(userSummary)


    def get_faculty(self,dept):
    #lookup faculty in orgchart file using department
        orgchart = "orgchart.csv"
        with open(orgchart, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                if dept == row[1]:
                    return row[7]
        return "Faculty not found"
        

def main():
    startDate = dt.datetime(2016,8,1)
    endDate = dt.datetime(2016,9,1)
    
    # Rudimentary method for submitting a comma separated list of usernames
    if len(argv) > 1:
        usernames = argv[1].split(",")

    kwargs = {
        "startDate": startDate,
        "endDate": endDate,
#        "platform": "Grace",
#        "usernames": ["uclxxxx"]
        }
    if usernames: kwargs["usernames"] = usernames

    jobHistory(**kwargs).makeResultsCSV()
        
if __name__ =="__main__":
    main()



