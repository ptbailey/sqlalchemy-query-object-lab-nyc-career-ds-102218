from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    company = Column(String)
    exchange = Column(String)
    symbol = Column(String)
    industry = Column(String)
    date_added = Column(String)
    enterprise_value = Column(Float)

engine = create_engine('sqlite:///dow_jones.db')
Base.metadata.create_all(engine) #this is creating your table. metadata object collects newly defined Table objects

Session = sessionmaker(bind=engine)
session = Session()

mmm = Company(company='3M',exchange='NYSE',symbol='MMM',industry='Conglomerate',date_added='1976-08-09',enterprise_value=133.31)
amx = Company(company='American Express',exchange='NYSE',symbol='AXP',industry='Financial services',date_added='1982-08-30',enterprise_value=98.08)
aapl = Company(company='Apple',exchange='NASDAQ',symbol='AAPL',industry='Technology',date_added='2015-03-19',enterprise_value=954.8)
ba = Company(company='Boeing',exchange='NYSE',symbol='BA',industry='Manufacturing',date_added='1987-03-12',enterprise_value=196.37)
cat = Company(company='Caterpillar',exchange='NYSE',symbol='CAT',industry='Manufacturing',date_added='1991-05-06',enterprise_value=118.42)
cvx = Company(company='Chevron',exchange='NYSE',symbol='CVX',industry='Oil & gas',date_added='2008-02-19',enterprise_value=264.51)
csco = Company(company='Cisco Systems',exchange='NASDAQ',symbol='CSCO',industry='Technology',date_added='2009-06-08',enterprise_value=174.31)
ko = Company(company='Coca-Cola',exchange='NYSE',symbol='KO',industry='Consumer products',date_added='1987-03-12',enterprise_value=216.95)
dwdp = Company(company='DowDuPont',exchange='NYSE',symbol='DWDP',industry='Manufacturing',date_added='2017-09-01',enterprise_value=179.4)
xom = Company(company='ExxonMobil',exchange='NYSE',symbol='XOM',industry='Oil & gas',date_added='1928-10-01',enterprise_value=371.97)
ge = Company(company='General Electric',exchange='NYSE',symbol='GE',industry='Conglomerate',date_added='1907-11-07',enterprise_value=232.12)
gs = Company(company='Goldman Sachs',exchange='NYSE',symbol='GS',industry='Financial services',date_added='2013-09-20',enterprise_value=-222.89)
hd = Company(company='The Home Depot',exchange='NYSE',symbol='HD',industry='Consumer products',date_added='1999-11-01',enterprise_value=225.3)
ibm = Company(company='IBM',exchange='NYSE',symbol='IBM',industry='Technology',date_added='1979-06-29',enterprise_value=178.63)
intc = Company(company='Intel',exchange='NASDAQ',symbol='INTC',industry='Technology',date_added='1999-11-01',enterprise_value=244.6)
jnj = Company(company='Johnson & Johnson',exchange='NYSE',symbol='JNJ',industry='Pharmaceuticals',date_added='1997-03-17',enterprise_value=366.73)
jpm = Company(company='JPMorgan Chase',exchange='NYSE',symbol='JPM',industry='Financial services',date_added='1991-05-06',enterprise_value=-118.82)
mcd = Company(company='McDonald\'s',exchange='NYSE',symbol='MCD',industry='Consumer products',date_added='1985-10-30',enterprise_value=155.49)
mrk = Company(company='Merck',exchange='NYSE',symbol='MRK',industry='Pharmaceuticals',date_added='1979-06-29',enterprise_value=174.1)
msft = Company(company='Microsoft',exchange='NASDAQ',symbol='MSFT',industry='Technology',date_added='1999-11-01',enterprise_value=708.61)
nke = Company(company='Nike',exchange='NYSE',symbol='NKE',industry='Consumer products',date_added='2013-09-20',enterprise_value=106.82)
pfe = Company(company='Pfizer',exchange='NYSE',symbol='PFE',industry='Pharmaceuticals',date_added='2004-04-08',enterprise_value=234.99)
pg = Company(company='Procter & Gamble',exchange='NYSE',symbol='PG',industry='Consumer products',date_added='1932-05-26',enterprise_value=217.15)
trv = Company(company='Travelers',exchange='NYSE',symbol='TRV',industry='Financial services',date_added='2009-06-08',enterprise_value=37.45)
unh = Company(company='UnitedHealth Group',exchange='NYSE',symbol='UNH',industry='Managed health care',date_added='2012-09-24',enterprise_value=233.23)
utx = Company(company='United Technologies',exchange='NYSE',symbol='UTX',industry='Conglomerate',date_added='1939-03-14',enterprise_value=117.52)
vz = Company(company='Verizon',exchange='NYSE',symbol='VZ',industry='Telecommunication',date_added='2004-04-08',enterprise_value=309.64)
v = Company(company='Visa',exchange='NYSE',symbol='V',industry='Financial services',date_added='2013-09-20',enterprise_value=280.04)
wmt = Company(company='Walmart',exchange='NYSE',symbol='WMT',industry='Consumer products',date_added='1997-03-17',enterprise_value=296.16)
dis = Company(company='Walt Disney',exchange='NYSE',symbol='DIS',industry='Broadcasting and entertainment',date_added='1991-05-06',enterprise_value=172.31)

companies = [mmm,amx,aapl,ba,cat,cvx,csco,ko,dwdp,xom,ge,gs,hd,ibm,intc,jnj,jpm,mcd,mrk,msft,nke,pfe,pg,trv,unh,utx,vz,v,wmt,dis]

if len(session.query(Company).all()) == 0:
    session.add_all(companies)
    session.commit()
