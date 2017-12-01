###
# This program examines whether a new record from archival sources can be identified
# as an existing person in the database.
#
# Input = "component table" from a local Access database
# Output = a table with identity recommendations for each component record,  
#   compared with the results of the manual identification for that record.
###

import pypyodbc 

# Global variables for DB connection
connection = None
cursor = None
connection_string = 'Driver={Microsoft Access Driver (*.mdb)};DBQ=C:\\Users\Tal\Documents\LocalData\PadgettlabLocal\FlorenceTest.mdb'
        

def TestComponentTable (starti = 1, stopi = 5167):
    # This function is used to run the tool and test it for a range of records in the table. 
    i = starti
    while i <= stopi:
        TestCR(i)
        i = i + 1        

def TestCR(id):
    # This function identifies a single new record from the component table.
    CR = ComponentRecord(id)
    if CR.CRexists and CR.id:
        CR.StandardizeAll()
        if CR.GetMatches(): CR.AssessMatches()
        CR.Recommend()
        return CR

class ComponentRecord:
    # An object of this class represents one new record from the component table. 
    
    def __init__(self, index):
    # Initialize an object and retrieve the data relevant for indentification.
        self.tblMaster = "tblMaster_040315"
        cursor.execute ('SELECT LNAME, FNAME, MNAME, M2NAME, NB, ID FROM tbl458catasto WHERE LINE_NUM = ' + str(index))
        rows = cursor.fetchall() 
        if not rows: 
            self.CRexists = False
            return
        else: self.CRexists = True
        for row in rows:
             self.lname = DataField (row[0]) # last name
             self.fname = DataField (row[1]) # first name
             self.mname = DataField (row[2]) # middle name 1
             self.m2name = DataField (row[3]) # middle name 2
             self.nb = DataField (row[4]) # neighborhood of residence
             self.id = row['id'] # the id of the historical record
             self.line_num = index
             self.year = 1458 # all records in this table are from the census of 1458.

    def StandardizeAll (self):
        # Standardize the data
        self.StandardizeLastName()
        if self.fname.dbvalue: self.StandardizeGivenName(self.fname)
        if self.mname.dbvalue: self.StandardizeGivenName(self.mname)
        if self.m2name.dbvalue: self.StandardizeGivenName(self.m2name)
        if self.nb.dbvalue: self.StandardizeNB()

    def StandardizeLastName (self):
        if self.lname.dbvalue:
            SplittedLname = self.lname.dbvalue.split(',',1)
            self.lname.svalue = SplittedLname[0].lower()
        else: self.lname.svalue = None
        
    def StandardizeGivenName (self, name):    
        # Standardize first and middle names based on the way these names have been 
        # most commonly standardized for existing records in the database.
        cursor.execute ("SELECT StandardName, Appearances FROM NS3 WHERE NonStandardName = '{0}' ORDER BY Appearances DESC".format(name.dbvalue))
        row = cursor.fetchone()
        if not row:
            print ("No standardized name for '{0}'".format(name.dbvalue))
        else: name.svalue = row[0].lower()

    def StandardizeNB (self):
        self.nb.svalue = self.nb.dbvalue

    def PrintCR (self):
        # Print database and standardized values. Used for debugging. 
        print ("lname db:{0:18} std:{1:8}".format(str(self.lname.dbvalue), str(self.lname.svalue)))
        print ("fname db:{0:18} std:{1:8}".format(str(self.fname.dbvalue), str(self.fname.svalue)))
        print ("mname  db:{0:18} std:{1:8}".format(str(self.mname.dbvalue), str(self.mname.svalue)))
        print ("m2name  db:{0:18} std:{1:8}".format(str(self.m2name.dbvalue), str(self.m2name.svalue)))
        print ("nb  db:{0:18} std:{1:8}".format(str(self.nb.dbvalue), str(self.nb.svalue)))

    def GetMatches (self):
        # Get from Master table data on all the existing people in the database with 
        # names similar to the component record
        if not self.fname.svalue:
            print('no fname')
            return False
        if self.lname.svalue:
            cursor.execute ("select * from {0} where smfname = '{1}' and mlname = '{2}' order by id, casen".format(self.tblMaster, self.fname.svalue, self.lname.svalue))
            self.matches = cursor.fetchall()
            if self.matches: return True
        if self.mname.svalue:
            cursor.execute ("select * from {0} where smfname = '{1}' and smmname = '{2}' order by id, casen".format(self.tblMaster, self.fname.svalue, self.mname.svalue))
            self.matches= cursor.fetchall()
            if not self.matches: 
                self.MRs = None
                return False
            else: return True

    def AssessMatches (self):
        # For each possible match, compute a score that assesses the quality of the match.
        self.MRs = []
        self.i = 0
        for row in self.matches:
            self.MRs.append(MasterRecord(row, self))

        for row in self.matches:
            if row['casen'] == 1: 
                self.MRs[self.i].NBs() # assess neighborhood fitness
                self.MRs[self.i].Names() # assess fitness 
                self.MRs[self.i].Years()
                self.MRs[self.i].overallfitness = self.MRs[self.i].nbfitness * self.MRs[self.i].namefitness * self.MRs[self.i].yearfitness
#               print("{0}   {1}    {2}    {3}      {4}".format(self.MRs[self.i].row['id'], self.MRs[self.i].nbfitness, self.MRs[self.i].namefitness, self.MRs[self.i].yearfitness, self.MRs[self.i].overallfitness))  
            self.i = self.i+1

    def Recommend (self):
        # Make a recommendation based on fitness scores and insert results in a table
        self.bestmatches = []
        maxfitness = 0
        self.noyears = False
        if not self.MRs:
            self.recommendation = "No Matches"
            self.correct = -1
        else:
            for mr in self.MRs:
                if mr.overallfitness >= .5 and mr.overallfitness == maxfitness: 
                    self.bestmatches.append(mr.row['id'])
                elif mr.overallfitness >= .5 and mr.overallfitness > maxfitness: 
                    self.bestmatches = [mr.row['id']]
                    maxfitness = mr.overallfitness
                elif mr.row['id'] == self.id: self.noyears = mr.noyears
            if len(self.bestmatches) == 1:
                self.recommendation = str(self.bestmatches[0])
                if self.bestmatches[0] == self.id: self.correct = 1
                else: self.correct = 0
            elif len(self.bestmatches) == 0: 
                self.recommendation = "No Matches"
                if self.noyears: self.correct = 1
                else: self.correct = 0
            else:
                self.recommendation = ""
                for match in self.bestmatches:
                    self.recommendation = self.recommendation + str(match) + "  "
                if self.id in self.bestmatches: self.correct = 1
                else: self.correct = 0

        cursor.execute("insert into 458MatchingResults values ({0}, '{1}', {2}, {3})".format(self.line_num, self.recommendation, self.id, self.correct)).commit()


class MasterRecord:
    # An object of this class represents one possible match from the Master table to the new record. 

    def __init__(self, row, CR):
        self.row = row
        self.CR = CR
        self.overallfitness = 0
        self.noyears = False  
      
    def NBs(self):
        # Assess the match of the neighborhood
        
        # Create a list of all the neighborhoods associated with the existing person
        self.nbs = []
        self.nbs.append(self.row['bngh'])
        self.nbs.append(self.row['ngh351'])
        self.nbs.append(self.row['scrut363ngh'])
        self.nbs.append(self.row['ngh378'])
        self.nbs.append(self.row['scrut382ngh'])
        self.nbs.append(self.row['scrut392ngh'])
        self.nbs.append(self.row['ngh403'])
        self.nbs.append(self.row['scrut411ngh'])
        self.nbs.append(self.row['ngh427'])
        self.nbs.append(self.row['scrut433ngh'])
        self.nbs.append(self.row['gonfngh'])
        self.nbs.append(self.row['ngh480'])
    
        self.nbfitness = 0.65
        for nb in self.nbs:
            if nb == self.CR.nb.svalue:
                self.nbfitness = 1
                return
            if nb: 
                self.nbfitness = 0.5
        if self.row['balia_quarter'] == int(self.CR.nb.svalue/10):
            self.nbfitness = 0.8
            return
        if self.row['balia_quarter']:
            self.nbfitness = 0.5 

    def Names(self):
        # Assess the match of the names       
        if not self.row['mlname'] or not self.CR.lname.svalue: self.lnamefit = None
        elif self.CR.lname.svalue in self.row['mlname'].lower(): self.lnamefit = True
        else: self.lnamefit = False 	
        if not self.row['smfname'] or not self.CR.fname.svalue: self.fnamefit = None
        elif self.row['smfname'].lower() == self.CR.fname.svalue: self.fnamefit = True
        else: self.fnamefit = False 	
        if not self.row['smmname'] or not self.CR.mname.svalue: self.mnamefit = None
        elif self.row['smmname'].lower() == self.CR.mname.svalue: self.mnamefit = True
        else: self.mnamefit = False 	
        if not self.row['smm2name'] or not self.CR.m2name.svalue: self.m2namefit = None
        elif self.row['smm2name'].lower() == self.CR.m2name.svalue: self.m2namefit = True
        else: self.m2namefit = False 	

        #This dictionary holds the decision making logic, based on the match of the 
        # last name, first name, and middle name. 
        DecisionMatrix = {
             (True, True, True):1,
             (True, True, None):.85,
             (None, True, True):.7,
             (False, True, True):.5,
             (True, False, True):.5,
             (True, True, False):.5
         }

        try:
            self.namefitness = DecisionMatrix[self.lnamefit, self.fnamefit, self.mnamefit]
            if self.m2namefit == True: self.namefitness = self.namefitness + .1
            if self.m2namefit == False: self.namefitness = self.namefitness - .1
        except KeyError:
            self.namefitness = 0
		

    def Years(self):
        # Assess the match of the years        
        self.married = []
        self.guildmatric = []
        self.politicaloffices = []
        self.taxcensuses = []
        self.otherrecords = []
        self.yearfitness = 0
        self.byr = self.row['byr_augm']
        if self.byr: self.byr = int(self.byr)
        self.dyr = self.row['dyr']
        if self.row['marr']: 
            self.married.append(self.row['marr'])
        case = 1
        while case < (len(self.CR.matches) - self.CR.i):
                if self.row['id'] == self.CR.MRs[self.CR.i + case].row['id']:
                    if self.CR.MRs[self.CR.i + case].row['marr']: self.married.append(self.CR.MRs[self.CR.i + case].row['marr'])
                    case = case+1
                else: break


        # Add all possible year references for a person
        if self.row['lanam']: self.guildmatric.append(self.row['lanam'])
        if self.row['ritagl_matr']: self.guildmatric.append(self.row['ritagl_matr'])
        if self.row['silkm']: self.guildmatric.append(self.row['silkm'])
        if self.row['calimm']: self.guildmatric.append(self.row['calimm'])
        if self.row['cambm']: self.guildmatric.append(self.row['cambm'])

        if self.row['prior1']: self.politicaloffices.append(self.row['prior1'])
        if self.row['buonuomini1']: self.politicaloffices.append(self.row['buonuomini1'])
        if self.row['gonfalonieri1']: self.politicaloffices.append(self.row['gonfalonieri1'])
        if self.row['balia1']: self.politicaloffices.append(self.row['balia1'])
        if self.row['consultepratiche1']: self.politicaloffices.append(self.row['consultepratiche1'])
        if self.row['acapir1']: self.politicaloffices.append(self.row['acapir1'])
        if self.row['capitani1']: self.politicaloffices.append(self.row['capitani1'])

        if self.row['ngh351']: self.taxcensuses.append(1351)
        if self.row['ngh378']: self.taxcensuses.append(1378)
        if self.row['ngh403']: self.taxcensuses.append(1403)
        if self.row['qt403']: self.taxcensuses.append(1403)
        if self.row['ngh427']: self.taxcensuses.append(1427)
#        if self.row['ngh458']: self.taxcensuses.append(1458)
        if self.row['ngh480']: self.taxcensuses.append(1480)

        if self.row['mercanzia']: self.otherrecords.append(self.row['mercanzia'])
        if self.row['lana']: 
            if self.row['lana'] < 1000: self.otherrecords.append(self.row['lana'] + 1000)     
            else: self.otherrecords.append(self.row['lana'])
        if self.row['calim1']: self.otherrecords.append(self.row['calim1'])
        if self.row['fpart']: self.otherrecords.append(self.row['fpart'])
        if self.row['nc427']: self.otherrecords.append(1427)
        if self.row['calimcon1']: self.otherrecords.append(self.row['calimcon1'])
        if self.row['cambcons1']: self.otherrecords.append(self.row['cambcons1'])
        if self.row['lanacons1']: self.otherrecords.append(self.row['lanacons1'])
        if self.row['setacons1']: self.otherrecords.append(self.row['setacons1'])
        if self.row['antmed_66']: self.otherrecords.append(1466)
        if self.row['mediceans_49']: self.otherrecords.append(1449)
        if self.row['scrut363ngh']: self.otherrecords.append(1363)
        if self.row['scrut382ngh']: self.otherrecords.append(1382)
        if self.row['scrut392ngh']: self.otherrecords.append(1392)
        if self.row['scrut411ngh']: self.otherrecords.append(1411)
        if self.row['scrut433ngh']: self.otherrecords.append(1433)

        self.allyears = self.married + self.guildmatric + self.politicaloffices + self.taxcensuses + self.otherrecords

        # Here comes the fit calculation. the logics may need to be adjusted for different component tables. 
        if self.byr and self.dyr:
            if self.byr < self.CR.year < self.dyr: self.yearfitness = 1 
        elif self.byr and not self.dyr and self.allyears: 
            if (self.byr < self.CR.year < max(self.allyears)) or (self.byr < self.CR.year < self.byr + 60): self.yearfitness = 1 
            elif self.byr < self.CR.year < self.byr + 80: self.yearfitness = .75 
            elif self.byr < self.CR.year < self.byr + 90: self.yearfitness = .5
        elif self.byr and not self.dyr and not self.allyears: 
            if self.byr < self.CR.year < self.byr + 60: self.yearfitness = 1 
            elif self.byr < self.CR.year < self.byr + 80: self.yearfitness = .75 
            elif self.byr < self.CR.year < self.byr + 90: self.yearfitness = .5
        elif not self.byr and self.dyr and self.allyears: 
            if (min(self.allyears) < self.CR.year < self.dyr) or (self.dyr - 50 < self.CR.year < self.dyr): self.yearfitness = 1 
            elif self.dyr - 70 < self.CR.year < self.dyr: self.yearfitness = .75 
            elif self.dyr - 80 < self.CR.year < self.dyr: self.yearfitness = .5         
        elif not self.byr and self.dyr and not self.allyears: 
            if (self.dyr - 50 < self.CR.year < self.dyr): self.yearfitness = 1 
            elif self.dyr - 70 < self.CR.year < self.dyr: self.yearfitness = .75 
            elif self.dyr - 80 < self.CR.year < self.dyr: self.yearfitness = .5         
        elif self.allyears: 
            if (max(self.allyears) - 50 < self.CR.year < min(self.allyears) + 50): self.yearfitness = 1
            elif (max(self.allyears) - 65 < self.CR.year < min(self.allyears) + 65): self.yearfitness = .75
            elif (max(self.allyears) - 80 < self.CR.year < min(self.allyears) + 80): self.yearfitness = .5
        else: self.noyears = True


class DataField:
    def __init__(self, value):
        self.dbvalue = value
        self.svalue = None

def InitDB():    
    connection = pypyodbc.connect(connection_string)
    cursor =  connection.cursor() 
    return connection

def CloseDB():
    cursor.close()
    connection.close()

        
        
        

InitDB()    
TestComponentTable()
CloseDB ()



