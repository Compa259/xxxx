from sqlalchemy import create_engine
import requests as rq
from common.database.sql_common import sql
from models.location_models import MappedProvince
from multiprocessing import Pool

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
        f'https://geocode.search.hereapi.com/v1/geocode?q={remove_accents(address)}&apiKey={HERE_API_KEY}').json()
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


def map_row(row):
    result = []
    for i in range(len(crm_provinces_name)):
        if int(row[9]) == int(crm_provinces_name[i]['id']):
            try:
                heremap_city = format_address(row[3])['city']
            except:
                print(row[3])
                heremap_city = None
            mapped_province = MappedProvince(
                id=row[0],
                d_city_id=row[1],
                d_domain_id=row[2],
                d_city_name=row[3],
                d_correct_city_name=heremap_city,
                d_value=row[4],
                d_id_region_in_domain=row[5],
                c_id=row[6],
                c_name=row[7],
                c_region_id=row[8],
                c_province_id=row[9],
                crm_province_name=crm_provinces_name[i]['name'],
            )
            result.append(mapped_province)
            break
    return result


if __name__ == '__main__':
    with open('crm_provinces.txt', 'r') as f:
        lines = f.readlines()

    crm_provinces_name = []
    for line in lines:
        x = line.split('#')
        crm_province_info = {'id': int(x[0]), 'name': x[1]}
        crm_provinces_name.append(crm_province_info)

    connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/crawler_db?charset=utf8'
    engine = create_engine(connection_str)
    sql_query = """select * from domain_city_info d left join city_pro c on c.id = toInt32(d.city_id) FORMAT TabSeparatedWithNamesAndTypes"""
    result = engine.execute(f'{sql_query}')
    records = result.fetchall()
    print(len(records))
    p = Pool(10)
    result = p.map(map_row, records)
    total_result = []
    for arr in result:
        total_result += arr
    sess = sql.get_session()
    sess.bulk_save_objects(total_result)
    sess.commit()
    sess.close()
