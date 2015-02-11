import Tkinter
import tkMessageBox
import datetime as dt
import stats.statsFactory as sf

class MainMenu(Tkinter.Tk):
    def __init__(self,parent):
        Tkinter.Tk.__init__(self,parent)
        self.parent = parent
        self.initialize()

    def initialize(self):
        self.grid()

        self.initializeDateSection()

        #node type selection
        nodeSelectLabel = Tkinter.Label(self,
                                        text="Node type:",
                                        fg="white",
                                        bg="blue",
                                        width="12",
                                        anchor="e").grid(column=0,row=3)
        self.nodeVar = Tkinter.StringVar()
        self.nodeVar.set('all')
        nodeList = list(sf.nodeDict.keys())
        nodeList.sort()
        nodeList.insert(0,"all")
        self.nodeSelector = Tkinter.OptionMenu(self, self.nodeVar, *nodeList)
        self.nodeSelector.grid(row=3,column=1)


        #Checkboxes to select statistics
        self.stats={}
        rowCount = 4
        for statistic in sf.Statistic.__subclasses__():
            self.stats[statistic.__name__] = Tkinter.IntVar()
            Tkinter.Checkbutton(self,text=statistic.__name__,
                                variable=self.stats[statistic.__name__]) \
                .grid(column=0,row=rowCount,sticky="W")
            rowCount += 1

        #Add OK button
        self.OK = Tkinter.Button(self, text="OK", command=self.OnOKClick) \
                  .grid(column=0,row=rowCount)

    def initializeDateSection(self):
        #Top labels
        self.MonthLabel = Tkinter.Label(self,
                                        text="Month (MM)",
                                        width="10")
        self.MonthLabel.grid(column=1,row=0)

        self.YearLabel = Tkinter.Label(self,
                                        text="Year (YYYY)",
                                        width="10")
        self.YearLabel.grid(column=2,row=0)


        #Start date bits
        self.startDateLabel = Tkinter.Label(self,
                                       text="Start Month:",
                                       fg="white",
                                       bg="blue",
                                       width="12",
                                       anchor="e")
        self.startDateLabel.grid(column=0,row=1)

        self.startMonth = Tkinter.StringVar()
        self.startMonthEntry = Tkinter.Entry(
            self,textvariable=self.startMonth,width="2")
        self.startMonthEntry.grid(column=1,row=1)

        self.startYear = Tkinter.StringVar()
        self.startYearEntry = Tkinter.Entry(
            self,textvariable=self.startYear,width="4")
        self.startYearEntry.grid(column=2,row=1)

        #End date bits
        self.endDateLabel = Tkinter.Label(self,
                                       text="End Month:",
                                       fg="white",
                                       bg="blue",
                                       width="12",
                                       anchor="e")
        self.endDateLabel.grid(column=0,row=2)

        self.endMonth = Tkinter.StringVar()
        self.endMonthEntry = Tkinter.Entry(
            self,textvariable=self.endMonth,width="2")
        self.endMonthEntry.grid(column=1,row=2)

        self.endYear = Tkinter.StringVar()
        self.endYearEntry = Tkinter.Entry(
            self,textvariable=self.endYear,width="4")
        self.endYearEntry.grid(column=2,row=2)
        

    def OnOKClick(self):
        if self.validateInput():
            startYear = int(self.startYear.get())
            startMonth = int(self.startMonth.get())
            endYear = int(self.endYear.get())
            endMonth = int(self.endMonth.get())
            startDate = dt.datetime(startYear,startMonth,1)
            endDate = dt.datetime(endYear,endMonth,1)
            node = self.nodeVar.get()
            stats = []
            for statistic in sf.Statistic.__subclasses__():
                if self.stats[statistic.__name__].get() == 1:
                    stats.append(statistic.__name__)
            kwargs = {
                "startDate":startDate,
                "endDate":endDate,
                "node":node,
                "stats":stats
                }
            getRequestedStats(**kwargs)

    def validateInput(self):
        if not self.validateMonth(self.startMonthEntry,self.startMonth.get()):
            return False
        if not self.validateMonth(self.endMonthEntry,self.endMonth.get()):
            return False
        if not self.validateYear(self.startYearEntry,self.startYear.get()):
            return False
        if not self.validateYear(self.endYearEntry,self.endYear.get()):
            return False
        if not self.validateDates():
            return False
        return True


    def validateMonth(self,widget,month):
        valid = True
        try:
            month = int(month)
        except ValueError:
            tkMessageBox.showerror("Input error!",
                    "You must enter an integer value.")
            valid = False
        if month < 1 or month > 12:
            tkMessageBox.showerror("Input error!",
                    "You must enter a value from 1 to 12.")
            valid = False
        if valid == False:
            widget.focus_set()
            widget.selection_range(0, Tkinter.END)
        return valid

    def validateYear(self,widget,year):
        valid = True
        try:
            year = int(year)
        except ValueError:
            tkMessageBox.showerror("Input error!",
                    "You must enter an integer value")
            valid = False
        if year < 2011 or year > 2050:
            tkMessageBox.showerror("Input error!",
                    "You must enter a value from 2011 to 2050.")
            valid = False
        if valid == False:
            widget.focus_set()
            widget.selection_range(0, Tkinter.END)
        return valid

    def validateDates(self):
        startDate = self.startYear.get() + self.startMonth.get()
        endDate = self.endYear.get() + self.endMonth.get()
        if startDate > endDate:
            tkMessageBox.showerror("Input error!",
                    "The start date must be earlier than or" \
                    +" equal to the end date.")
            self.startMonthEntry.focus_set()
            self.startMonthEntry.selection_range(0, Tkinter.END)
            return False
        else:
            return True

def getRequestedStats(**kwargs):
    intervals = getMonthIntervals(kwargs["startDate"], kwargs["endDate"])
    monthCollection = []
    for interval in intervals:
        kwargs["startDate"] = interval["startDate"]
        kwargs["endDate"] = interval["endDate"]
        statsDict = {}
        for statistic in kwargs["stats"]:
            statsDict[statistic] = sf.statFactory(statistic, **kwargs) \
                                   .getResult()
        statsDict["Start Date"] = kwargs["startDate"].strftime('%d-%m-%Y')
        statsDict["End Date"] = kwargs["endDate"].strftime('%d-%m-%Y')
        statsDict["Node Type"] = kwargs["node"]
        statsDict = flatten(statsDict)
        monthCollection.append(statsDict)
    printStats(monthCollection)

def printStats(collection):
    tempCollection = collection
    for month in tempCollection:
        print "Start Date: %s" % (month["Start Date"])
        del month["Start Date"]
        print "End Date: %s" % (month["End Date"])
        del month["End Date"]
        print "Node Type: %s" % (month["Node Type"])
        del month["Node Type"]
        for key in sorted(month):
            print "{0}: {1}".format(key,month[key])
    return 0

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

def getMonthIntervals(startDate, endDate):
    intervals = []
    while startDate != add_months(endDate,1):
        interval = {
            "startDate":startDate,
            "endDate":add_months(startDate,1)
            }
        intervals.append(interval)
        startDate = add_months(startDate,1)
    return intervals

def add_months(sourcedate,months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month / 12
    month = month % 12 + 1
    day = 1
    return dt.datetime(year,month,day)

if __name__=="__main__":
    app = MainMenu(None)
    app.title('my application')
    app.mainloop()
