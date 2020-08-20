from common.database.sql_common import sql
from models.location_models import MappedProvinceTestDucnm
from sqlalchemy import create_engine
import mysql.connector
import re


def no_accent_vietnamese(s):
    s = re.sub('[áàảãạăắằẳẵặâấầẩẫậ]', 'a', s)
    s = re.sub('[éèẻẽẹêếềểễệ]', 'e', s)
    s = re.sub('[óòỏõọôốồổỗộơớờởỡợ]', 'o', s)
    s = re.sub('[íìỉĩị]', 'i', s)
    s = re.sub('[úùủũụưứừửữự]', 'u', s)
    s = re.sub('[ýỳỷỹỵ]', 'y', s)
    s = re.sub('đ', 'd', s)
    return s


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
        crm_province_info = {'id': int(x[0]), 'name': x[2], 'no_access_name': no_accent_vietnamese(x[2])}
        crm_provinces_name.append(crm_province_info)

    #get data mapping domain_city_info vs city
    connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/crawler_db?charset=utf8'
    engine = create_engine(connection_str)
    sql_query = """select * from (
               select * from domain_city_info d left join city c on c.id = toInt32(d.city_id)
                  ) where domain_id=5"""
    result = engine.execute(f'{sql_query} FORMAT TabSeparatedWithNamesAndTypes')
    records = result.fetchall()
    insert_mapped_province = []
    for row in records:
        check_value = False
        mapped_province = str(row[7]).strip()
        for i in range(len(crm_provinces_name)):
            if mapped_province == crm_provinces_name[i]['name']:
                province_info = MappedProvinceTestDucnm(
                    id=row[0],
                    d_city_id=row[1],
                    d_domain_id=row[2],
                    d_city_name=row[3],
                    d_value=row[4],
                    d_id_region_in_domain=row[5],
                    c_id=row[6],
                    c_name=row[7],
                    c_region_id=row[8],
                    crm_province_id=crm_provinces_name[i]['id'],
                    crm_province_name=crm_provinces_name[i]['name']
                )
                check_value = True
                insert_mapped_province.append(province_info)
                break
            else:
                location_details = row[3].split(',')
                if str(location_details[1]).strip() == crm_provinces_name[i]['name'] or str(location_details[1]).strip() == crm_provinces_name[i]['no_access_name']\
                        or str(location_details[0]).strip() == crm_provinces_name[i]['name'] or str(location_details[0]).strip() == crm_provinces_name[i]['no_access_name']:
                    province_info = MappedProvinceTestDucnm(
                        id=row[0],
                        d_city_id=row[1],
                        d_domain_id=row[2],
                        d_city_name=row[3],
                        d_value=row[4],
                        d_id_region_in_domain=row[5],
                        c_id=row[6],
                        c_name=row[7],
                        c_region_id=row[8],
                        crm_province_id=crm_provinces_name[i]['id'],
                        crm_province_name=crm_provinces_name[i]['name']
                    )
                    check_value = True
                    insert_mapped_province.append(province_info)
                    break
        if not check_value:
            print(row[3])
    # sess = sql.get_session()
    # sess.bulk_save_objects(insert_mapped_province)
    # sess.commit()
    # sess.close()
            # insert_mapped_province.append(MappedProvinceTestDucnm)

        # if mapped_province not in crm_provinces_name:
        #     location_details = row[3].split(',')
        #     if str(location_details[1]).strip() not in crm_provinces_name:
        #         # Set truong hop ko dau:
        #         no_access_provinces = []
        #         for element in crm_provinces_name:
        #             no_access_provinces.append(no_accent_vietnamese(element))
        #         if str(location_details[1]).strip() not in no_access_provinces:
        #             print(row[3])
        #             print('----------------')






    # Logic:
        # so sanh neu city_name nam trong list province thi lay luon

        # neu ko thì split , lay gia tri thu 2

    # Save