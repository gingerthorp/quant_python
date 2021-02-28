import requests as rq
from bs4 import BeautifulSoup
import re
import requests as rq
from io import BytesIO
import pandas as pd
import os
import time
import json
from tqdm import tqdm

if not os.path.exists('data'):
    os.makedirs('data')

# 최근 영업일 구하기
url = 'https://finance.naver.com/sise/sise_deposit.nhn'
data = rq.get(url)
data_html = BeautifulSoup(data.content, "html5lib")
parse_day = data_html.select_one('div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
biz_day = re.findall("[0-9]+", parse_day)
biz_day = "".join(biz_day)

print('최근 영업일:' + biz_day)

# 코스피 업종분류 데이터 다운로드
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

gen_otp_data = {
  'mktId': 'STK',
  'trdDd': '20210108',
  'money': '1',
  'csvxls_isNo': 'false',
  'name': 'fileDown',
  'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}

headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}           

otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text   

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_KS  = rq.post(down_url, {'code':otp}, headers=headers)

sector_KS = pd.read_csv(BytesIO(down_sector_KS.content), encoding='EUC-KR')

# 코스닥 업종분류 데이터 다운로드
gen_otp_data = {
  'mktId': 'KSQ',
  'trdDd': '20210108',
  'money': '1',
  'csvxls_isNo': 'false',
  'name': 'fileDown',
  'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
}

otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text         

down_sector_KQ  = rq.post(down_url, {'code':otp}, headers=headers)
sector_KQ = pd.read_csv(BytesIO(down_sector_KQ.content), encoding='EUC-KR')

# 합치기
down_sector = pd.concat([sector_KS, sector_KQ]).reset_index(drop=True)
down_sector['종목명'] = down_sector['종목명'].str.strip()

# 저장하기
down_sector.to_csv('data/krx_sector.csv')

# 개별종목 지표 OTP 발급
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'

gen_otp_data = {
  'searchType' : '1',
  'mktId' : 'ALL',
  'trdDd' : '20210108',
  'csvxls_isNo' : 'false',
  'name' : 'fileDown',
  'url' : 'dbms/MDC/STAT/standard/MDCSTAT03501'
}

otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text         

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_ind  = rq.post(down_url, {'code':otp}, headers=headers)

down_ind = pd.read_csv(BytesIO(down_ind.content), encoding='EUC-KR')
down_ind['종목명'] = down_ind['종목명'].str.strip()

# 저장하기
down_ind.to_csv('data/krx_ind.csv')

# 데이터 정리하기
KOR_ticker = pd.merge(down_sector, down_ind, on = (down_sector.columns & down_ind.columns).tolist(), how = 'inner')
KOR_ticker = KOR_ticker.sort_values(by = ['시가총액'], ascending = False)
KOR_ticker = KOR_ticker[~KOR_ticker['종목명'].str.contains('스팩')]
KOR_ticker = KOR_ticker[KOR_ticker['종목코드'].str[-1:] == '0']
KOR_ticker = KOR_ticker.reset_index(drop=True)
KOR_ticker.to_csv('data/KOR_ticker.csv')

print('티커 정보가 다운로드 되었습니다.')

# 섹터정보 다운로드
sector_code = ['G25', 'G35', 'G50', 'G40', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45']

data_sector = {}

for i in tqdm(sector_code):
  url = 'http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt='+biz_day+'&sec_cd='+i
  data = rq.get(url).json()
  data_pd = pd.json_normalize(data['list'])

  data_sector[i] = data_pd

  time.sleep(2)

sector_list = [v for k,v in data_sector.items()] 
KOR_sector = pd.concat(sector_list).reset_index(drop=True)

KOR_sector.to_csv('data/KOR_sector.csv')
print('티커 정보가 다운로드 되었습니다.')
input("엔터를 누르면 프로그램이 종료됩니다.")
