import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
ALPHA_KEY       = os.getenv("ALPHA_VANTAGE_API_KEY", "")
AV_BASE_URL     = "https://www.alphavantage.co/query"
# 108611


SQL_USER   = os.getenv("SQL_USER")
SQL_PWD    = os.getenv("SQL_PWD")
SQL_HOST   = os.getenv("SQL_HOST")
SQL_PORT   = os.getenv("SQL_PORT")
SQL_DB_NAME= os.getenv("SQL_DB_NAME")
DATABASE_URL = f"postgresql://{SQL_USER}:{SQL_PWD}@{SQL_HOST}:{SQL_PORT}/{SQL_DB_NAME}"
engine        = create_engine(DATABASE_URL, echo=False)
SessionLocal  = sessionmaker(bind=engine)
