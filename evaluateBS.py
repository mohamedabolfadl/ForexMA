# -*- coding: utf-8 -*-
"""
Created on Wed Jun 07 08:14:32 2017

@author: m00760171
"""

import csv 
from sklearn.neural_network import MLPClassifier
#def getDateDifferenceTrigger(currDt, dt_x):
    
    

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
        if curr in dt_s:
            # We have an on going trade
            if trade:
                results_s.append((-1,curr)) 
            else:
                    # Our price is the closing price
                    pri = float(currDate[5])-BS_key*spread
                    triggerDate = curr
                    trade = True
    feu.close()  
    
    # Open trade will just be discarded
    if trade:
        results_s.append((-1,triggerDate))
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
        if curr in dt_b:
            # We have an on going trade
            if trade:
                results_b.append((-1,curr)) 
            else:
                # Our price is the closing price
                pri = float(currDate[5])+BS_key*spread
                triggerDate = curr
                trade = True
    if trade:
        results_b.append((0,triggerDate))
    if abs((len(dt_s)-len(results_s)))>0:
        print "Mismatch in selling signals of "+fileName[7:13]
        print str(len(dt_s))+ " sell signals"
        print str(len(results_s))+ " results"
       
    if abs((len(dt_b)-len(results_b)))>0:
        print "Mismatch in buying signals of "+fileName[7:13]
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
[EUb, EUs] = getResults("DAT_MT_EURUSD_M1_201705.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 0.7
target = 10
[GUb, GUs] = getResults("DAT_MT_GBPUSD_M1_201705.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.01
spread = 1
target = 10
[UJb, UJs] = getResults("DAT_MT_USDJPY_M1_201705.csv", pipsize, spread, target,1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[AUb, AUs] = getResults("DAT_MT_AUDUSD_M1_201705.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[UHb, UHs] = getResults("DAT_MT_USDCHF_M1_201705.csv", pipsize, spread, target,1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[NUb, NUs] = getResults("DAT_MT_NZDUSD_M1_201705.csv", pipsize, spread, target,-1,dt_b,dt_s)

pipsize = 0.0001
spread = 1
target = 10
[UCb, UCs] = getResults("DAT_MT_USDCAD_M1_201705.csv", pipsize, spread, target,1,dt_b,dt_s)


# Getting time of the day in minutes from 00:00
buyTimes = []
for dt in dt_b:
    #buyTime = float(dt[11:13])*60+float(dt[14:])
    buyTimes.append(float(dt[11:13])*60+float(dt[14:]))
sellTimes = []
for dt in dt_s:
    sellTimes.append(float(dt[11:13])*60+float(dt[14:]))


# Compiling buy results

if not (len(EUs) == len(GUs) ==len(UJs) ==len(AUs) ==len(UHs) ==len(NUs) ==len(UCs)):
    print "Sells results mismatch"
    
if not (len(EUb) == len(GUb) ==len(UJb) ==len(AUb) ==len(UHb) ==len(NUb) ==len(UCb)):
    print "Buys results mismatch"
    
    
i = 0
#Compiling sells
features_s=[['St_E_s','St_G_s','St_J_s','St_A_s','St_H_s','St_N_s','St_C_s','Time_day_s','Time_diff_s']]
res_s = [['res_E','res_G','res_J','res_A','res_H','res_N','res_C']]
while i<len(EUs):
    curr_res = [EUs[i][0],GUs[i][0],UJs[i][0],AUs[i][0],UHs[i][0],NUs[i][0],UCs[i][0]]
         
    if not (-1 in curr_res):
        features_s.append([stre_s[i],strg_s[i],strj_s[i],stra_s[i],strh_s[i],strn_s[i],strc_s[i], sellTimes[i], diffDate_s[i] ])    
        res_s.append(curr_res)
    i = i + 1




i = 0
#Compiling buys
features_b=[['St_E_b','St_G_b','St_J_b','St_A_b','St_H_b','St_N_b','St_C_b','Time_day_b','Time_diff_b']]
res_b = [['res_E','res_G','res_J','res_A','res_H','res_N','res_C']]
while i<len(EUb):
    curr_res = [EUb[i][0],GUb[i][0],UJb[i][0],AUb[i][0],UHb[i][0],NUb[i][0],UCb[i][0]]  
    if not (-1 in curr_res):
        features_b.append([stre_b[i],strg_b[i],strj_b[i],stra_b[i],strh_b[i],strn_b[i],strc_b[i], buyTimes[i], diffDate_b[i] ])    
        res_b.append(curr_res)
    i = i + 1







# Machine learning
#clf = MLPClassifier(solver='lbfgs', alpha=1e-5,hidden_layer_sizes=(8,), random_state=1)
#clf.fit(features_b, res_b)













