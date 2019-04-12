"""
    File: taiwan.py
    Author: hsuanshao@gmail.com
    Create Date: 2019/1/14
    Purpose: Index crawer to taiwan stock market
"""
import time
import requests
import pandas as pd
import json
import re
from pandas.io.json import json_normalize
from datetime import datetime

class Taiwan:
    def __init__(self, year, month, date):
        self.year = year
        self.month = month
        self.date = date
    
    def dateFormatCheck(self):
        return False


    def transDateToStr(self):
        year = str(self.year)
        month = str(self.month)
        date = str(self.date)

        if self.month < 10:
            month = "0" + str(self.month)
        if self.date < 10:
            date = "0" + str(self.date)
        return year+month+date

    def transDateToMS(self, strdate):
        transactionTime = datetime.strptime(strdate, "%Y%m%d")
        transactionTimeMs = int(round(transactionTime.timestamp() * 1000))
        return transactionTimeMs

    def getURL(self, url, param):
        try:
            r = requests.get(url, params=param)
        except Exception as e:
            print(type(e), ":", str(e))
            return False
        self.response = r
        return True

    def getRequst(self):
        try:
           df = json_normalize(self.response.json())
        except:
            return False
        self.df = df
        return True
    
    def transStrToCol(self, input):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', input)
        if cleantext == None or cleantext == "--" or cleantext == 'nan' or input == '':
            cleantext = "0"
        return cleantext
    
    def getMIindex(self):
        """
            台股每日收盤指數
        """
        datestring = self.transDateToStr()
        
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'
        params = {
            'date': datestring,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time()*1000)-500)
        }

        urlResponse = self.getURL(url, params)
        if urlResponse == False:
            return "URL response error"
        
        jsonSource = self.getRequst()
        if jsonSource == False:
            return "Transfer json error"

        # stock transaction date
        transactionTimeMs = self.transDateToMS(datestring)

        exchangeData = pd.DataFrame(data=self.df) 
        for _, row in exchangeData.iterrows():
            tyear = int(datestring[0:4])
            tmonth = int(datestring[4:6])
            fieldstr = "data5"

            if tyear <= 2008:
                fieldstr = "data2"

            if tyear > 2008 and tyear <= 2011:
                fieldstr = "data4"

            if tyear == 2011 and tmonth >= 8:
                fieldstr = "data5"
 
            stockTrade = []
            
            if fieldstr in row:
                stockTrade = row[fieldstr]
                if row["date"] != datestring:
                    return "time error" + datestring + ":" + row["date"]
        index = 0
        result = []
        
        while index < len(stockTrade):
            """
            0: 證券代號
            1: 證券名稱
            2: 交易時間 (transacrtionTimeMs)
            3: 成交股數
            4: 成交筆數
            5: 成交金額
            6: 開盤價
            7: 最高價
            8: 最低價
            9: 收盤價
            10: 漲跌(+/-)
            11: 漲跌價差
            12: 最後揭示買價
            13: 最後揭示買量
            14: 最後揭示賣價
            15: 最後揭示賣量
            16: P/E 本益比
            """
            stockCode = stockTrade[index][0]
            companyName = stockTrade[index][1]
            # 2 成交股數
            tv = str(stockTrade[index][2]).replace(",","")
            tv = self.transStrToCol(tv)
            tradingVolume = int(tv)
            # 3 成交筆數
            nOt = str(stockTrade[index][3]).replace(",","")
            nOt = self.transStrToCol(nOt)
            numberOfTransactions = int(nOt)
            # 4 成交金額
            toiv = str(stockTrade[index][4]).replace(",","")
            toiv = self.transStrToCol(toiv)
            turnOverInValue = float(toiv)
            # 5 開盤價
            opening = str(stockTrade[index][5]).replace(",","")
            opening = self.transStrToCol(opening)
            openingPrice = float(opening)
            # 6 最高價
            highest = str(stockTrade[index][6]).replace(",","")
            highest = self.transStrToCol(highest)
            highestPrice = float(highest)
            # 7 最低價
            lowest = str(stockTrade[index][7]).replace(",","")
            lowest = self.transStrToCol(lowest)
            lowestPrice = float(lowest)
            # 8 收盤價
            closing = str(stockTrade[index][8]).replace(",","")
            closing = self.transStrToCol(closing)
            closingPrice = float(closing)
            # 9 漲跌(+/-)
            direction = str(stockTrade[index][9])
            cleanr = re.compile('<.*?>')
            direction = re.sub(cleanr, '', direction)
            # 10 漲跌價差
            pc = str(stockTrade[index][10]).replace(",","")
            pc = self.transStrToCol(pc)
            priceChange = float(pc)
            # 11 最後揭示買價
            lbbp = str(stockTrade[index][11]).replace(",","")
            lbbp = self.transStrToCol(lbbp)
            lastBestBidPrice = float(lbbp)
            # 12 最後揭示買量
            lbn = str(stockTrade[index][12]).replace(",","")
            lbn = self.transStrToCol(lbn)
            lastBidNumber = int(lbn)
            # 13 最後揭示賣價
            lbap = str(stockTrade[index][13]).replace(",","")
            lbap = self.transStrToCol(lbap)
            lastBestAskPrice = float(lbap)
            # 14 最後揭示賣量
            lan = str(stockTrade[index][14]).replace(",","")
            lan = self.transStrToCol(lan)
            lastAskNumber = int(lan)
            # 15 P/E 本益比
            pe = str(stockTrade[index][15]).replace(",","")
            pe = self.transStrToCol(pe)
            PERate = float(pe)
            
            tmpList = [stockCode, companyName, transactionTimeMs, tradingVolume, numberOfTransactions, turnOverInValue, openingPrice, highestPrice, lowestPrice, closingPrice, direction, priceChange, lastBestBidPrice, lastBidNumber, lastBestAskPrice, lastAskNumber, PERate]
            
            result.append(tmpList)
            index += 1
        return result

# taiwan = Taiwan(2011,9,6)
# result = taiwan.getMIindex()