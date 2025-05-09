import os
from dotenv import load_dotenv
from sqlmodel import create_engine, SQLModel, Session
from models.User import User  # Import this first
from models.File import File  # Then import File
# Load environment variables
load_dotenv()

# Get database connection parameters from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# Create database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Print the database URL for debugging purposes (optional)
print(f"Database URL: {DATABASE_URL}")

engine = create_engine(DATABASE_URL)

# Export SQLModel as Base for table creation
Base = SQLModel

def get_session():
    with Session(engine) as session:
        yield session