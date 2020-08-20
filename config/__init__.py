class Config:
    AEROSPIKE_HOST = '13.251.43.182'
    AEROSPIKE_PORT = 3000
    AEROSPIKE_KEY_SPACE = 'test'
    SIMILAR_HOTEL_ENDPOINT = 'http://18.140.113.157:5000/api/similars'
    MASTER_SEARCH_ENDPOINT = 'http://3.0.202.87:8088/get_price'
    FACT_SET = 'on_demand'
    RAW_SET = 'on_demand_raw'
    DATABASE_DRIVER = 'clickhouse+native'
    # DATABASE_USERNAME = 'crawler_db'
    # DATABASE_PASSWORD = 'm6MVBJWBcjBZhPsjMFJl'
    # DATABASE_HOST = '13.251.26.93'
    # DATABASE_PORT = '8123'
    # DATABASE_NAME = 'crawler_db'
    DATABASE_USERNAME = 'streamsets'
    DATABASE_PASSWORD = 'bWqFHseP8KjIZw+RhzQL'
    DATABASE_HOST = '172.31.25.244'
    DATABASE_PORT = '8123'
    DATABASE_NAME = 'crawler_db'
    # BUCKET_NAME = 'tripi-data-singapore'

    def get_value(self, value):
        return getattr(self, value)


config = Config()
