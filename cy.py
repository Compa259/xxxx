import pandas as pd
import sqlalchemy


def dim_hotel_mytour():
    engine = sqlalchemy.create_engine('clickhouse://crawler_db:m6MVBJWBcjBZhPsjMFJl@13.229.34.221:8128/crawler_db?charset=utf8')
    df = pd.read_excel('mapping_mytour.xlsx', sheet_name='dim_hotel_mytour')
    df = df[['id','name','star','type','priority','system','address','district','province','root_province_id','market','email','tick_like','tick_penalty','tick_low_quality','tick_active','hot_special_type_note','hot_lat','hot_lng']]
    df = df[df['name'].notnull()]
    print(df)
    df.to_sql('dim_hotel_mytour', con=engine, if_exists='append', chunksize=1000, index=False)

dim_hotel_mytour()