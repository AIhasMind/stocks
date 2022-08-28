import pandas as pd
from bs4 import BeautifulSoup
import requests
import calendar, json
from sqlalchemy import create_engine
from datetime import datetime
from threading import Timer
import pymysql
from dotenv import load_dotenv
import os

load_dotenv()


# 테스트 패치
class DBUpdater:
    """일별 시세를 매일 DB로 업데이트한다."""

    ###############################################################
    # table 생성
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드dict 생성"""
        # pandas.read_sql로 불러오는 경우가 아닐 때는
        # sqlalchemy로 우회하지 않고 pymysql 만으로 조회코드를 작성한다
        self.conn = pymysql.connect(host=os.environ['MY_DB_HOST'], port=int(os.environ['MY_DB_PORT']),
                                 db=os.environ['MY_DB_NAME'], user=os.environ['MY_DB_USER'],
                                 passwd=os.environ['MY_DB_PASSWORD'], autocommit=True, charset='utf8')
        # pandas.read_sql로 불러오는 경우 pymysql.connect는 오류가 나는데,
        # 이 경우, sqlalchemy의 engine을 대신 사용하려 한다.
        self.engine = create_engine('mysql+pymysql://root:{}@{}:{}/{}'.format(
                        os.environ['MY_DB_PASSWORD'], os.environ['MY_DB_HOST'],
                        os.environ['MY_DB_PORT'], os.environ['MY_DB_NAME']))

        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code));
            """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date));
            """
            curs.execute(sql)

        self.conn.commit()

        # 비어있는 종목코드dict 생성
        self.codes = dict()
        self.update_comp_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    ################################################################
    # 종목코드
    def read_krx_code(self):
        """KRX에서 상장법인목록 읽어 DataFrame으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=' \
              'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드', '회사명']]
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:06d}'.format)
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트하고 dict에 저장"""
        sql = "SELECT * FROM company_info;"
        # self.conn이 오류! engine을 사용.
        # 보통은 connect()없이 다음처럼 사용하지만,
        # https: // docs.sqlalchemy.org / en / 13 / core / connections.html
        # 또는 pd.read_sql(sql, self.engine.connect())도 되더라.
        # engine.connect()는 Connection 객체이므로 close()가 필요하거나
        # context manager(with as)를 이용한다.
        df = pd.read_sql(sql, self.engine)
        # codes dict를 만들어 채운다.
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]

        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]
                    sql = f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}', '{company}', '{today}')"
                    curs.execute(sql)
                    self.codes[code] = company
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] {idx:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")
                    self.conn.commit()
                    print('')

    ################################################################
    # 시세
    def read_naver(self, code, company, pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 DataFrame으로 반환"""
        try:
            ### 1. 맨 뒤 페이지 숫자 구하기 ###
            url = f"https://finance.naver.com/item/sise_day.naver?code={code}"
            headers = {'User-agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            if response.ok:
                soup = BeautifulSoup(response.text, "lxml")
                pgrr = soup.find("td", class_="pgRR")
                if pgrr is None:
                    return None
                s = str(pgrr.a["href"]).split('=')
                lastpage = s[-1]
            else:
                return None
            ###############################

            ### 2. 맨 뒤 페이지 수와 가져올 페이지 수를 이용하여 시세 데이터를 읽음.###
            df = pd.DataFrame()
            pages = min(int(lastpage), pages_to_fetch)
            for page in range(1, pages + 1):
                pg_url = '{}&page={}'.format(url, page)
                response = requests.get(pg_url, headers=headers)
                page_data = pd.read_html(response.text, header=0)[0]
                df = pd.concat([df, page_data], ignore_index=True)
                ### 다운로스 상황 보고 ###
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                # 같은 자리에서 교체되면서 출력 end="\r"
                print('[{}] {} ({}): {:04d}/{:04d} pages are downloading...'.
                      format(tmnow, company, code, page, pages), end='\r')
                #########################
            ###################################################################

            ### 3. 읽어 들인 데이터 정돈 ###
            df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff',
                                    '시가': 'open', '고가': 'high', '저가': 'low',
                                    '거래량': 'volume'})
            # df의 date칼럼의 날짜표시형식을 표준방식으로 변경한다. datetime64 타입이 된다.
            # https://stackoverflow.com/questions/38067704/how-to-change-the-datetime-format-in-pandas
            df.date = pd.to_datetime(df['date'])

            # NaN 항목이 drop된다. 그러면 index항목 숫자가 건너 뛰면서 표시된다는 점에 유의.
            df = df.dropna()
            df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close',
                                                                         'diff', 'open', 'high', 'low',
                                                                         'volume']].astype(int)
            df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
            ##################

        except Exception as e:
            print('Exception occured: ', str(e))
            return None
        return df

    def replace_into_db(self, df, idx, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 replace"""
        with self.conn.cursor() as curs:
            for r in df.itertuples():
                sql = """
                REPLACE INTO daily_price VALUES (
                    '{}', '{}', '{}', '{}',
                    '{}', '{}', '{}', '{}');
                """.format(code, r.date, r.open, r.high,
                           r.low, r.close, r.diff, r.volume)
                print(sql)
                curs.execute(sql)
            self.conn.commit()
            print('[{}] #{:04d} {} ({}): {} rows > REPLACE INTO daily_' \
                  'price [OK]'.format(datetime.now().strftime('%Y-%m-%d' \
                                                              ' %H:%M'), idx + 1, company, code, len(df)))

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버에서 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            # if idx > 4:  # debug
            #     break
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    ###############################################################
    # 반복 worker
    def execute_daily(self):
        """실행 즉시 및 매일 오후 5시에 daily_price 테이블 업데이트"""
        self.update_comp_info()
        try:
            with open('config.json', 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100  # debug
                config = {'pages_to_fetch': 1}
                json.dump(config, out_file)

        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.today == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ...".format(tmnext.strftime
                                                        ('%Y-%m-%d %H:%M')))
        t.start()


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()