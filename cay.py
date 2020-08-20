import requests as rq
import mysql.connector
from sqlalchemy import create_engine

from common.database.sql_common import sql
from models.location_models import MappedProvinceTestDucnm, City

s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞở' \
     u'ỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ '
s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOo' \
     u'OoOoOoUuUuUuUuUuUuUuYyYyYyYy '
HERE_API_KEY = 'Qi5ftLvc9a1YFNu83tkoQt1OiyYsyRNOfJONn7V0P7g'


def remove_accents(input_str):
    s = ''
    for c in input_str:
        if c in s1:
            s += s0[s1.index(c)]
        else:
            s += c
    return s.lower().replace(',', '')


def format_address(address):
    data = rq.get(
        f'https://geocode.search.hereapi.com/v1/geocode?q={address}&apiKey={HERE_API_KEY}').json()
    try:
        detail_address = data['items'][0]['address']
        return_value = {
            'formatted_address': detail_address.get('label'),
            'city': detail_address.get('county'),
            'district': detail_address.get('city'),
            'wards': detail_address.get('district'),
            'street': detail_address.get('street'),
        }
    except:
        print(f"Can't find address: {address}")
        return {}
    return return_value


if __name__ == '__main__':
    # lay list province cua crm
    mydb = mysql.connector.connect(
        host="35.198.217.65",
        user="okr",
        password="okr-tripi@1231",
        database="crm"
    )

    mycursor = mydb.cursor()

    mycursor.execute("SELECT * FROM province WHERE country_id = 1")

    myresult = mycursor.fetchall()

    crm_provinces_name = []
    for x in myresult:
        crm_province_info = {'id': int(x[0]), 'name': x[2]}
        crm_provinces_name.append(crm_province_info)
    connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/crawler_db?charset=utf8'
    # connection_str = 'clickhouse+native://streamsets:bWqFHseP8KjIZw+RhzQL@172.31.25.244:9000/crawler_db?charset=utf8'
    engine = create_engine(connection_str)
    sql_query = """select * from city"""
    # result = engine.execute(f'{sql_query} FORMAT TabSeparatedWithNamesAndTypes')
    result = engine.execute(f'{sql_query}')
    records = result.fetchall()
    print(len(records))
    insert_mapped_province = []
    for row in records:
        for i in range(len(crm_provinces_name)):
            if str(row[1]).strip() == crm_provinces_name[i]['name']:
                province_info = City(
                    id=row[0],
                    name=row[1],
                    region_id=row[2],
                    province_id=crm_provinces_name[i]['id']
                )
                insert_mapped_province.append(province_info)
                break
    sess = sql.get_session()
    sess.bulk_save_objects(insert_mapped_province)
    sess.commit()
    sess.close()

