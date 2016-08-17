from tkinter import *
import datetime as dt
import statsFactory as sf

class MainMenu(Tk):
    def __init__(self,parent):
        Tk.__init__(self,parent)
        self.parent = parent
        self.initialize()

    def initialize(self):
        self.grid()

        self.initializePlatformSelector()

        self.initializeDateSection()

        #node type selection
        nodeSelectLabel = Label(self,
                                text="Node type:",
                                fg="white",
                                bg="blue",
                                width="12",
                                anchor="e").grid(column=0,row=4)
        self.nodeVar = StringVar()
        self.nodeVar.set('all')
        nodeList = list(sf.nodeDict.keys())
        nodeList.sort()
        nodeList.insert(0,"all")
        self.nodeSelector = OptionMenu(self, self.nodeVar, *nodeList)
        self.nodeSelector.grid(row=4,column=1)


        #Checkboxes to select statistics
        self.stats={}
        rowCount = 5
        for statistic in sf.Statistic.__subclasses__():
            self.stats[statistic.__name__] = IntVar()
            Checkbutton(self,text=statistic.__name__,
                        variable=self.stats[statistic.__name__]) \
                .grid(column=0,row=rowCount,sticky="W")
            rowCount += 1

        #Add OK button
        self.OK = Button(self, text="OK", command=self.OnOKClick) \
                  .grid(column=0,row=rowCount)

    def initializePlatformSelector(self):
        #platform selector
        platformSelectLabel = Label(self,
                                    text="Platform:",
                                    fg="white",
                                    bg="blue",
                                    width="12",
                                    anchor="e").grid(column=0,row=0)
        self.platformVar = StringVar()
        self.platformVar.set('Legion')
        platformList = list(sf.platform2database.keys())
        self.platformSelector = OptionMenu(self, self.platformVar, *platformList)
        self.platformSelector.grid(row=0,column=1)

    def initializeDateSection(self):
        #Top labels
        self.MonthLabel = Label(self,
                                text="Month (MM)",
                                width="10")
        self.MonthLabel.grid(column=1,row=1)

        self.YearLabel = Label(self,
                               text="Year (YYYY)",
                               width="10")
        self.YearLabel.grid(column=2,row=1)


        #Start date bits
        self.startDateLabel = Label(self,
                                    text="Start Month:",
                                    fg="white",
                                    bg="blue",
                                    width="12",
                                    anchor="e")
        self.startDateLabel.grid(column=0,row=2)

        self.startMonth = StringVar()
        self.startMonthEntry = Entry(
            self,textvariable=self.startMonth,width="2")
        self.startMonthEntry.grid(column=1,row=2)

        self.startYear = StringVar()
        self.startYearEntry = Entry(
            self,textvariable=self.startYear,width="4")
        self.startYearEntry.grid(column=2,row=2)

        #End date bits
        self.endDateLabel = Label(self,
                                  text="End Month:",
                                  fg="white",
                                  bg="blue",
                                  width="12",
                                  anchor="e")
        self.endDateLabel.grid(column=0,row=3)

        self.endMonth = StringVar()
        self.endMonthEntry = Entry(
            self,textvariable=self.endMonth,width="2")
        self.endMonthEntry.grid(column=1,row=3)

        self.endYear = StringVar()
        self.endYearEntry = Entry(
            self,textvariable=self.endYear,width="4")
        self.endYearEntry.grid(column=2,row=3)
        

    def OnOKClick(self):
        if self.validateInput():
            platform = self.platformVar.get()
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
                "stats":stats,
                "db":sf.platform2database[platform]
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
            messagebox.showerror("Input error!",
                    "You must enter an integer value.")
            valid = False
        if month < 1 or month > 12:
            messagebox.showerror("Input error!",
                    "You must enter a value from 1 to 12.")
            valid = False
        if valid == False:
            widget.focus_set()
            widget.selection_range(0, END)
        return valid

    def validateYear(self,widget,year):
        valid = True
        try:
            year = int(year)
        except ValueError:
            messagebox.showerror("Input error!",
                    "You must enter an integer value")
            valid = False
        if year < 2011 or year > 2050:
            messagebox.showerror("Input error!",
                    "You must enter a value from 2011 to 2050.")
            valid = False
        if valid == False:
            widget.focus_set()
            widget.selection_range(0, END)
        return valid

    def validateDates(self):
        startDate = self.startYear.get() + self.startMonth.get()
        endDate = self.endYear.get() + self.endMonth.get()
        if startDate > endDate:
            messagebox.showerror("Input error!",
                    "The start date must be earlier than or" \
                    +" equal to the end date.")
            self.startMonthEntry.focus_set()
            self.startMonthEntry.selection_range(0, END)
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
        print("Start Date: {}".format(month["Start Date"]))
        del month["Start Date"]
        print("End Date: {}".format(month["End Date"]))
        del month["End Date"]
        print("Node Type: {}".format(month["Node Type"]))
        del month["Node Type"]
        for key in sorted(month):
            print("{0}: {1}".format(key,month[key]))
    return 0

def flatten(statsDict):
    toDelete = []
    toAdd = {}
    for key, value in statsDict.items():
        if type(value) is dict:
            for k, v in value.items():
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
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = 1
    return dt.datetime(year,month,day)

if __name__=="__main__":
    app = MainMenu(None)
    app.title('my application')
    app.mainloop()
