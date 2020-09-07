# -*- coding: UTF-8 -*-
import csv
import datetime
import os
import logging

import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup

# -*-*-*-**-***-*-**-*-
# -* Logging Setting *-
# -*-*-*-*-*-*-*-*--*-*
# create logger
logger = logging.getLogger('institute_crawler')
logger.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")

# create hadler 控制輸出 console 部分
chlr = logging.StreamHandler()
chlr.setLevel(logging.INFO)
chlr.setFormatter(formatter)

# create hadler 控制寫入 file 部分
path_log = os.path.join('log')
if not(os.path.isdir(path_log)):
    os.makedirs(path_log)
fhlr = logging.FileHandler(os.path.join(path_log, 'institute_crawler.log'))
fhlr.setLevel(logging.INFO)
fhlr.setFormatter(formatter)

# add handler
logger.addHandler(chlr)
logger.addHandler(fhlr)


# *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# *-*-*-*-*-*-*-*-*-*FUNCTION PLACE*-*-*-*-*-*-*-*-*-*-*-*-
# *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
def day_data_clean(sql_cursor):
    # 先做商品、需要的資訊篩選
    temp_file_path = './daily_temp_data.csv'
    df = pd.read_csv(temp_file_path, encoding='utf-8')
    # df = df[df['商品名稱'] == '臺股期貨']
    df = df.drop(
        columns=['多方交易契約金額(千元)',
                 '空方交易契約金額(千元)',
                 '多空交易契約金額淨額(千元)',
                 '多方未平倉契約金額(千元)',
                 '空方未平倉契約金額(千元)',
                 '多空未平倉契約金額淨額(千元)'])
    df_tx = df[df['商品名稱'] == '臺股期貨']
    df_mtx = df[df['商品名稱'] == '小型臺指期貨']

    # 寫入大台法人交易資料
    for data in df_tx.values:
        # 輸入SQL語法時要注意，date 與 字串 都需要加上''來表示。
        sql = "INSERT INTO tx_institute_daily (日期, 身分別, 多方交易口數, 空方交易口數, 多空交易口數淨額, 多方未平倉口數, 空方未平倉口數, 多空未平倉口數淨額)" \
              "VALUES ('{0}','{1}',{2},{3},{4},{5},{6},{7});".format(data[0],
                                                                     data[2],
                                                                     data[3],
                                                                     data[4],
                                                                     data[5],
                                                                     data[6],
                                                                     data[7],
                                                                     data[8])
        sql_cursor.execute(sql)
    # 寫入小台法人交易資料
    for data in df_mtx.values:
        # 輸入SQL語法時要注意，date 與 字串 都需要加上''來表示。
        sql = "INSERT INTO mtx_institute_daily (日期, 身分別, 多方交易口數, 空方交易口數, 多空交易口數淨額, 多方未平倉口數, 空方未平倉口數, 多空未平倉口數淨額)" \
              "VALUES ('{0}','{1}',{2},{3},{4},{5},{6},{7});".format(data[0],
                                                                     data[2],
                                                                     data[3],
                                                                     data[4],
                                                                     data[5],
                                                                     data[6],
                                                                     data[7],
                                                                     data[8])
        sql_cursor.execute(sql)


def crawl_data(target_day, last_upgrade_day, sql_cursor):
    while 1:

        # 能讀取的最舊資料日期：
        if target_day.strftime('%Y/%m/%d') == last_upgrade_day:
            logger.debug('爬蟲操作完畢！')
            break
        # 逢六日跳過：
        elif (target_day.weekday() == 5) or (target_day.weekday() == 6):
            target_day = target_day - datetime.timedelta(days=1)
            continue
        # 正常日：
        else:
            target_day_str = target_day.strftime('%Y/%m/%d')

        data = {
            'firstDate': target_day_str,
            'lastDate': target_day_str,
            'queryStartDate': target_day_str,
            'queryEndDate': target_day_str,
            'commodityId': ''
        }
        res = requests.post(url, headers=header, data=data)
        soup = BeautifulSoup(res.text, 'html.parser')
        text = soup.text

        # 先偵測撈到的資料時間是否有異常
        time_error_msg = '日期時間錯誤 DateTime error'
        null_error_msg = '查無資料'

        if (time_error_msg in text) or (null_error_msg in text):
            logger.info('沒有' + target_day_str + '這天的資料')
        else:
            text = text.split('\r\n')

            c = open('daily_temp_data.csv', 'w', encoding='utf-8', newline='')
            for each_row in text[:-1]:
                temp_ls = each_row.split(',')
                writer = csv.writer(c)
                writer.writerow(temp_ls)
            c.close()
            logger.info('the data of date  ' + target_day_str + '  is downloaded........')

            # 將爬下來的資料整理進總表
            day_data_clean(sql_cursor)
        # 時間往後一天
        target_day = target_day - datetime.timedelta(days=1)


# *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# *-*-*-*-*-*-*-*-*-*-MAIN PROGRAM*-*-*-*-*-*-*-*-*-*-*-*-*
# *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
# 【日期】
today = datetime.datetime.now().strftime('%Y/%m/%d')

# 建立今日執行 log
logger.info(f'***************【{today} 執行紀錄】***************')


# 【爬蟲基本參數】
url = 'https://www.taifex.com.tw/cht/3/futContractsDateDown'
header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Length': '135',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'JSESSIONID=EC0D4697F9A4C784639FE54E77C05BFA.tomcat2; _ga=GA1.3.372105997.1564380943; BIGipServerPOOL_WWW_TCP_80=487985324.20480.0000; ROUTEID=.tomcat2; BIGipServerPOOL_iRule_WWW_ts50search=420876460.20480.0000; _gid=GA1.3.1249016116.1567395901; BIGipServerPOOL_iRule_WWW_Group=437653676.20480.0000',
    'Host': 'www.taifex.com.tw',
    'Origin': 'http://www.taifex.com.tw',
    'Referer': 'https://www.taifex.com.tw/cht/3/futContractsDateView',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'
}
target_day = datetime.datetime.now()

logger.debug('【台指期三大法人資料爬蟲】\n')
logger.debug('正在檢查資料庫，確認歷史資料中........\n')

# database connect info.
connection = pymysql.connect(host='127.0.0.1',
                             port=3306,
                             user='********',
                             password='********',
                             db='FUTURES',
                             charset='utf8')
#
cursor = connection.cursor()

cursor.execute("SELECT 日期 "
               "FROM futures.tx_institute_daily "
               "order by 日期 desc "
               "limit 1;")
# 取得資料庫最新資料日期
last_upgrade_day = cursor.fetchone()

# 確認資料庫是否有資料，有就接續補上，無則從頭開始補
if last_upgrade_day is None:
    # 這裡寫死預設最遠爬取日期
    last_upgrade_day = '2016/03/10'
    logger.debug("資料庫尚未有資料，開始進行寫入........")
    crawl_data(target_day, last_upgrade_day, cursor)
else:
    last_upgrade_day = last_upgrade_day[0].strftime('%Y/%m/%d')  # 原資料型態為tuple，在這邊做轉換
    logger.debug("今天日期為：　　", today)
    logger.debug("上次更新日期為：", last_upgrade_day, "\n")
    logger.debug('開始爬取資料...............\n')
    crawl_data(target_day, last_upgrade_day, cursor)

connection.commit()
connection.close()

try:
    os.remove('./daily_temp_data.csv')
except FileNotFoundError:
    pass
