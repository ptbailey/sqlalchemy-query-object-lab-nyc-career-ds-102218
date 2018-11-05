from sqlalchemy import * #create_engine, func
from seed import Company
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return ((session.query(Company.company).filter_by(company = 'Apple')).all()).pop()

def return_disneys_industry():
    return ''.join(((session.query(Company.industry).filter_by(symbol='DIS').all())).pop())

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    ordered = session.query(Company.company,Company.enterprise_value).\
    filter(Company.industry == 'Technology').order_by(Company.enterprise_value.desc()).all()
    return [{'company':x[0], 'EV': x[1]} for x in ordered]

def return_list_of_consumer_products_companies_with_EV_above_225():
    ordered = session.query(Company.company).filter(Company.industry\
     == 'Consumer products').filter(Company.enterprise_value > 225).all()
    return [{'name':tup[0]} for tup in ordered]

def return_conglomerates_and_pharmaceutical_companies():
    ordered = session.query(Company.company).filter(or_(Company.industry == 'Conglomerate', \
    Company.industry == 'Pharmaceuticals')).all()
    return [tup[0] for tup in ordered]

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value).label('Avg Enterprise Value')).all()

def return_industry_and_its_total_EV():
    return session.query(Company.industry,func.sum(Company.enterprise_value).\
    label('Total Enterprise Value By Industry')).group_by(Company.industry).\
    order_by(Company.industry).all()
