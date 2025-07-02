# Import necessary packages for database modeling and data handling

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer,String, DateTime, Float

Base=declarative_base()

# This is the schema, a class that represents eviction records with reasons, dates, and location details.

class Eviction(Base):
    __tablename__='eviction'
    id=Column(Integer, primary_key=True, autoincrement=True)
    eviction_id=Column(String)
    address=Column(String)
    city=Column(String)
    state=Column(String)
    eviction_notice_zipcode=Column(Integer)
    file_date=Column(DateTime)
    non_payment=Column(Integer)
    breach=Column(Integer)
    nuisance=Column(Integer)
    illegal_use=Column(Integer)
    failure_to_sign_renewal=Column(Integer)
    access_denial=Column(Integer)
    unapproved_subtenant=Column(Integer)
    owner_move_in=Column(Integer)
    demolition=Column(Integer)
    capital_improvement=Column(Integer)
    substantial_rehab=Column(Integer)
    ellis_act_withdrawal=Column(Integer)
    condo_conversion=Column(Integer)
    roomate_same_unit=Column(Integer)
    other_cause=Column(Integer)
    late_payment=Column(Integer)
    lead_remediation=Column(Integer)
    development=Column(Integer)
    good_samaritan_ends=Column(Integer)
    constraints_date=Column(DateTime)
    data_as_of=Column(DateTime)
    data_loaded_at=Column(DateTime)
    location_latitude=Column(Float)
    location_longitude=Column(Float)
    shape_latitude=Column(Float)
    shape_longitude=Column(Float)

    def __repr__(Self):
        return {Self.eviction_id}

