from functools import partial
from multiprocessing import Pool, cpu_count

from sqlalchemy import create_engine, orm

from common.helper.data import split_array
from config import config


class SqlCommon:
    def __init__(self, driver='clickhouse', host=None, port=8123,
                 username=None, password=None, database='default'):

        connection_str = f'{driver}://' \
                         f'{username}:{password}' \
                         f'@{host}:{port}/' \
                         f'{database}' \
                         f'?charset=utf8'
        self.engine = create_engine(connection_str)

    def get_session(self):
        if self.engine is not None:
            Session = orm.sessionmaker()
            Session.configure(bind=self.engine)
            session = Session()
            return session
        return None

    def quit(self):
        self.engine.dispose()

    def execute(self, query):
        return self.engine.execute(f'{query} FORMAT TabSeparatedWithNamesAndTypes')

    def insert_bulk(self, data: list, part=100, session=None):
        splited_data = split_array(data, part)
        for d in splited_data:
            if session is None:
                session = self.get_session()
            session.bulk_save_objects(d)
            session.flush()
            session.close()

    def insert_with_high_performance(self, data, part=100):
        """
        only use in main process not in thread or subprocess
        :param data:
        :param part:
        :return:
        """
        pool = Pool(cpu_count())
        pool.map(partial(self.insert_bulk, part=part, session=None), data)
        pool.close()


def init_sql():
    driver = config.get_value('DATABASE_DRIVER')
    username = config.get_value('DATABASE_USERNAME')
    password = config.get_value('DATABASE_PASSWORD')
    host = config.get_value('DATABASE_HOST')
    port = config.get_value('DATABASE_PORT')
    db_name = config.get_value('DATABASE_NAME')
    return SqlCommon(driver=driver, host=host, port=port,
                     username=username, password=password, database=db_name)


sql = init_sql()
