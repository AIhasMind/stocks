from db import analyzer

mk = analyzer.MarketDB()
print(mk.get_daily_price('삼성전자', '2022-03-01', '2022-07-31')['close'])