import datetime

from common.database.sql_common import sql
from models.location_models import DomainCityInfo

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from clickhouse_driver import Client
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)

client = gspread.authorize(creds)

if __name__ == '__main__':
    clickhouse_client = Client(host='172.31.25.244', port=9000,
                              user='streamsets',
                              password='bWqFHseP8KjIZw+RhzQL',
                              database='tripi_data')
    #connection_str = 'clickhouse://ducnm:H7R67ciSgvcyxCNodD9c@13.229.34.221:8128/tripi_data?charset=utf8'
    sheet = client.open('mapped_province').get_worksheet(0)
    rows = sheet.get_all_records()
    insert_mapped_province = []
    for row in rows:
        try:
            domain_city_info = DomainCityInfo(
                id = row['id'],
                domain_id = row['d_domain_id'],
                city_id = row['d_city_id'],
                city_name = row['d_city_name'],
                value = row['d_value'],
                id_region_in_domain = row['d_id_region_in_domain'],
                root_province_id = row['crm_province_id']
            )
            insert_mapped_province.append(domain_city_info)
        except:
            pass

    sess = sql.get_session()
    sess.bulk_save_objects(insert_mapped_province)
    sess.commit()
    sess.close()