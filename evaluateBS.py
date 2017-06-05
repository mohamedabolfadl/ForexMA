# -*- coding: utf-8 -*-
"""
Created on Mon Jun 05 15:26:50 2017

@author: Moh2
"""
import csv 



def getResults(fileName, pipsize, spread, target, BS_key,dt_b,dt_s):
    feu = open(fileName, "rb")
#    pipsize = 0.0001
    spread = spread*pipsize
    target = target*pipsize
    reader = csv.reader(feu)    
    
    trade = False
    results_s = []
    pri = 0
    
    for currDate in reader:
        curr = currDate[0]+" "+currDate[1]
        
        # We have an open position
        if trade:
            # High and low of current candle
            hi = float(currDate[3])
            lo = float(currDate[4])
            
            if lo< (pri-target) :
                trade = False
                #print str(curr)+" in low"
                if BS_key>0:
                    results_s.append((1,triggerDate))  
                else:
                    results_s.append((0,triggerDate))  
                    
    
            if hi> (pri+target):
                trade = False
                #print curr+" in high, pri:"+str(pri+target)+" "
                if BS_key>0:
                    results_s.append((0,triggerDate))  
                else:
                    results_s.append((1,triggerDate)) 
          
            
        # Now is a trigger   
        if curr in dt_s and not trade:
            # Our price is the closing price
            pri = float(currDate[5])-BS_key*spread
            triggerDate = curr
            trade = True
    feu.close()  
    
    if trade:
        results_s.append((0,triggerDate))
    ##################################################################
    feu = open(fileName, "rb")
    reader = csv.reader(feu)
    
    
    trade = False
    results_b = []
    pri = 0
    for currDate in reader:
        curr = currDate[0]+" "+currDate[1]
        # We have an open position
        if trade:
            # High and low of current candle
            hi = float(currDate[3])
            lo = float(currDate[4])
            
            if lo< (pri-target) :
                trade = False
                if BS_key>0:
                    results_b.append((0,triggerDate))  
                else:
                    results_b.append((1,triggerDate))   
    
    
            if hi> (pri+target):
                trade = False
                if BS_key>0:
                    results_b.append((1,triggerDate))  
                else:
                    results_b.append((0,triggerDate))  
          
            
        # Now is a trigger   
        if curr in dt_b and not trade:
            # Our price is the closing price
            pri = float(currDate[5])+BS_key*spread
            triggerDate = curr
            trade = True
    if trade:
        results_b.append((0,triggerDate))
    if abs((len(dt_s)-len(results_s)))>0:
        print "Mismatch in selling signals of "+fileName[0:6]
        print str(len(dt_s))+ " sell signals"
        print str(len(results_s))+ " results"
       
    if abs((len(dt_b)-len(results_b)))>0:
        print "Mismatch in buying signals of "+fileName[0:6]
        print str(len(dt_b))+ " buy signals"
        print str(len(results_b))+ " results"  
    
    return [results_b, results_s]
    
# Reading Buy and sell signals
fs = open("Sell_USD_MA33_M5_Tr15_Thr121.csv", "rb")
reader = csv.reader(fs)
dt_s = []
stre_s=[]
strc_s=[]
strg_s=[]
strj_s=[]
strn_s=[]
strh_s=[]
stra_s=[]
diffDate_s = []
for row in reader:
    rawdt = row[0]
    stre_s.append(float(row[1]))
    strc_s.append(float(row[2]))
    strg_s.append(float(row[3]))
    strj_s.append(float(row[4]))
    strn_s.append(float(row[5]))
    strh_s.append(float(row[6]))
    stra_s.append(float(row[7]))
    diffDate_s.append(float(row[9]))    
    dt_time = rawdt.split(' ')
    tmp = dt_time[1]
    ins = dt_time[0].replace('-','.')+' '+tmp[0:5]
    dt_s.append(ins)
 
 
fb = open("Buy_USD_MA33_M5_Tr15_Thr121.csv", "rb")
reader = csv.reader(fb)
stre_b=[]
strc_b=[]
strg_b=[]
strj_b=[]
strn_b=[]
strh_b=[]
stra_b=[]
diffDate_b = []
dt_b=[]
for row in reader:
    rawdt = row[0]
    stre_b.append(float(row[1]))
    strc_b.append(float(row[2]))
    strg_b.append(float(row[3]))
    strj_b.append(float(row[4]))
    strn_b.append(float(row[5]))
    strh_b.append(float(row[6]))
    stra_b.append(float(row[7]))
    diffDate_b.append(float(row[9]))
    dt_time = rawdt.split(' ')
    tmp = dt_time[1]
    ins = dt_time[0].replace('-','.')+' '+tmp[0:5]
    dt_b.append(ins)
fb.close()
fs.close()


# Reading All currency pairs

pipsize = 0.0001
spread = 0.7
target = 10
[EUb, EUs] = getResults("EURUSDs.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 0.7
target = 10
[GUb, GUs] = getResults("GBPUSDs.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.01
spread = 1
target = 10
[UJb, UJs] = getResults("USDJPYs.csv", pipsize, spread, target,1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[AUb, AUs] = getResults("AUDUSDs.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[UHb, UHs] = getResults("USDCHFs.csv", pipsize, spread, target,1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[NUb, NUs] = getResults("NZDUSDs.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[UCb, UCs] = getResults("USDCADs.csv", pipsize, spread, target,1,dt_b,dt_s)


# Getting time of the day in minutes from 00:00
buyTimes = []
for dt in dt_b:
    buyTime = float(dt[11:13])*60+float(dt[14:])
    buyTimes.append(float(dt[11:13])*60+float(dt[14:]))
sellTimes = []
for dt in dt_s:
    sellTimes.append(float(dt[11:13])*60+float(dt[14:]))


# Compiling buy results





