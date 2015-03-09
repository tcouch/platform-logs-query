import statsFactory as sf
import csv, sys
import datetime as dt

def main():
    if updateRecord():
        print "Adding last month's stats to stats_record.csv"
        recordUpdate()
    else:
        printRange()

def specifyRecordUpdate(startDate, endDate):
    print "Fetching stats for period beginning {0} and ending {1}..." \
              .format(startDate, endDate)
    stats = getStats(startDate, endDate)
    printStats(stats)
    updateCSV(stats)
    print "stats_record.csv has been updated"
    return 0

def recordUpdate():
    startDate, endDate = lastMonthDates()
    print "Fetching stats for period beginning {0} and ending {1}..." \
              .format(startDate, endDate)
    stats = getStats(startDate, endDate)
    printStats(stats)
    updateCSV(stats)
    print "stats_record.csv has been updated"
    return 0

def updateCSV(stats):
    with open('stats_record.csv', 'r+b') as f:
        header = next(csv.reader(f))
        dict_writer = csv.DictWriter(f, header, -999)
        dict_writer.writerow(stats)

def lastMonthDates():
    now = dt.datetime.now()
    if now.day > 3:
        startMonth, startYear = subtractMonth(now.month, now.year, 1)
        endMonth, endYear = now.month, now.year
    else:
        startMonth, startYear = subtractMonth(now.month, now.year, 2)
        endMonth, endYear = subtractMonth(now.month, now.year, 1)
        print "PLEASE NOTE: You must wait five days from the" \
              + " start of the month before the previous month's" \
              + " statistics can be calculated."
    startDate = "{0}-{1}-01".format(startYear, startMonth)
    endDate = "{0}-{1}-01".format(endYear, endMonth)
    return startDate, endDate

def subtractMonth(month, year, num):
    month -= num
    if month < 1:
        month += 12
        year -= 1
    return month, year

def updateRecord():
    inputCheck = False
    while inputCheck == False:
        selection = raw_input("Press 'U' to Update the stats record with" \
                          + " last month's stats, press 'S' to print" \
                          + " stats from a Specific period, or leave" \
                          + " blank to quit: ")
        if selection == "U" or selection == "u":
            inputCheck = True
            return True
        elif selection == "S" or selection == "s":
            inputCheck = True
            return False
        elif selection == "":
            inputCheck = True
            sys.exit()
        else:
            print "That was not a valid response, please try again."

def printRange():
    repeat = True
    while repeat == True:
        startDate, endDate = getDates()
        print "Fetching stats for period beginning {0} and ending {1}..." \
              .format(startDate, endDate)
        stats = getStats(startDate, endDate)
        printStats(stats)
        inputCheck = False
        while inputCheck == False:
            again = raw_input("Would you like to see results for another" \
                              + " date range? (Y/N) ")
            if again == "Y" or again == "y":
                inputCheck = True
            elif again == "N" or again == "n":
                inputCheck = True
                repeat = False
            else:
                print "Please enter 'Y' or 'N'"
    sys.exit
    
def getStats(startDate,endDate,**kwargs):
    nullwrite = NullWriter()
    oldstdout = sys.stdout
    sys.stdout = nullwrite
    kwargs["startDate"] = dt.datetime.strptime(startDate, '%Y-%m-%d')
    kwargs["endDate"] = dt.datetime.strptime(endDate, '%Y-%m-%d')
    statsDict = {}
    for statistic in sf.Statistic.__subclasses__():
        statsDict[statistic.__name__] = \
            sf.statFactory(statistic.__name__, **kwargs).getResult()    
    statsDict = flatten(statsDict)
    statsDict["Start Date"] = startDate
    statsDict["End Date"] = endDate
    sys.stdout = oldstdout
    return statsDict

def flatten(statsDict):
    toDelete = []
    toAdd = {}
    for key, value in statsDict.iteritems():
        if type(value) is dict:
            for k, v in value.iteritems():
                toAdd["{0}-{1}".format(key, k)] = v
            toDelete.append(key)
    for key in toDelete:
        del statsDict[key]
    statsDict.update(toAdd)
    return statsDict

def printStats(stats):
    for key, value in stats.iteritems():
        print "{0}: {1}".format(key, value)
    return 0

def getDates():
    datesCheck = False
    while datesCheck == False:
        dateCheck = False
        while dateCheck == False:
            startDate = raw_input("Please enter the start date for the period" \
                                 + " you would like to look at (YYYY-MM-DD): ")
            dateCheck = checkDate(startDate)
        dateCheck = False
        while dateCheck == False:
            endDate = raw_input("Please enter the end date for the period" \
                                 + " you would like to look at (YYYY-MM-DD): ")
            dateCheck = checkDate(endDate)
        if endDate > startDate:
            datesCheck = True
        else:
            print "You have entered an end date which is before the start date." \
                  + " Please try again."
    return startDate, endDate

def checkStartBeforeEnd(start,end):
    if dt.datetime.strptime(start, '%Y-%m-%d') >= \
       dt.datetime.strptime(end, '%Y-%m-%d'):
        return False
    else:
        return True

def checkDate(date):
    try:
        dt.datetime.strptime(date, '%Y-%m-%d')
        return True
    except ValueError:
        print("Incorrect date format, should be YYYY-MM-DD")
        return False

class NullWriter(object):
    def write(self, arg):
        pass

if __name__=="__main__":
    main()
