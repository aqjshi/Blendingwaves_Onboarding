from sqlalchemy import (
    Column, Integer, BigInteger, Float, UniqueConstraint, ForeignKey, text, DateTime, String
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.dialects.postgresql import JSONB


Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    id     = Column(Integer, primary_key=True)
    ticker = Column(String(10), unique=True, index=True, nullable=False)
    quotes      = relationship("Quote", back_populates="company")
    subsecond_quotes = relationship("SubsecondQuote", back_populates="company") 
    historical_emulate_quotes = relationship("HistoricalEmulateQuote", back_populates="company") 

class Quote(Base):
    __tablename__ = "quotes"

    id             = Column(BigInteger, primary_key=True)
    company_id     = Column(Integer, ForeignKey("companies.id"),
                              nullable=False, index=True)
    # point the FK to time_entries.unix_ts
    time_entry_ts  = Column(BigInteger,
                             nullable=False, index=True)

    open_price     = Column(Float)
    high_price     = Column(Float)
    low_price      = Column(Float)
    close_price    = Column(Float)
    volume         = Column(BigInteger)

    company        = relationship("Company",   back_populates="quotes")

    __table_args__ = (
        UniqueConstraint("company_id", "time_entry_ts",
                         name="uq_company_timeentry"),
    )
class SubsecondQuote(Base): 
    __tablename__ = "subsecond_quotes"
    id = Column(BigInteger, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    time_entry_ts = Column(BigInteger, nullable=False, index=True) # Unix timestamp in microseconds/nanoseconds
    close_price = Column(Float)
    volume = Column(BigInteger)
    company = relationship("Company", back_populates="subsecond_quotes")

    __table_args__ = (
        UniqueConstraint("company_id", "time_entry_ts", name="uq_company_timeentry_subsecond"),
    )

class HistoricalEmulateQuote(Base): 
    __tablename__ = "historical_emulate_quotes"
    id = Column(BigInteger, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, index=True)
    time_entry_ts = Column(BigInteger, nullable=False, index=True) 
    close_price = Column(Float)
    volume = Column(BigInteger)
    company = relationship("Company", back_populates="historical_emulate_quotes")

    __table_args__ = (
        UniqueConstraint("company_id", "time_entry_ts", name="uq_company_timeentry_historical"),
    )

class TrainItem(Base):
    __tablename__ = "train_item"
    id            = Column(BigInteger, primary_key=True, autoincrement=True)
    time_entry_ts = Column(BigInteger,
                           nullable=False, index=True)

    # input
    tensor = Column(JSONB)

    # timelne 
    a_time = Column(JSONB)
    s_time = Column(JSONB)
    t_time = Column(JSONB)
    b_time = Column(JSONB)
    a_value = Column(JSONB)
    s_value = Column(JSONB)  
    t_value  = Column(JSONB) 
    b_value = Column(JSONB)  
    
    __table_args__ = (
        UniqueConstraint('time_entry_ts', name='_time_entry_uc'),
    )




