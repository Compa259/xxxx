import sqlalchemy.types as T
from sqlalchemy import Column

from common.database.base import Base


class MappedProvince(Base):
    __tablename__ = 'mapped_province'
    id = Column(T.Integer(), primary_key=True)
    d_city_id = Column(T.Integer())
    d_domain_id = Column(T.String())
    d_city_name = Column(T.Integer())
    d_value = Column(T.Integer())
    d_id_region_in_domain = Column(T.String())
    c_id = Column(T.String())
    c_name = Column(T.String())
    c_region_id = Column(T.Integer())
    c_province_id = Column(T.Integer())
    crm_province_name = Column(T.String())


class City(Base):
    __tablename__ = 'city_pro'
    id = Column(T.Integer(), primary_key=True)
    name = Column(T.String())
    region_id = Column(T.Integer())
    province_id = Column(T.Integer())


class DomainCityInfo(Base):
    __tablename__ = 'domain_city_info_pro'
    id = Column(T.Integer(), primary_key=True)
    city_id = Column(T.Integer())
    domain_id = Column(T.Integer())
    city_name = Column(T.String())
    value = Column(T.Integer())
    id_region_in_domain = Column(T.String())
    root_province_id = Column(T.Integer())