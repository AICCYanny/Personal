import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv('DB_URL')

engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)