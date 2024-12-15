import time
import random
import uuid
import logging
import boto3
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func, text, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base configuration
Base = declarative_base()

# Product Model
class Product(Base):
    __tablename__ = 'products'
    
    id = Column(UUID, primary_key=True, default=text('gen_random_uuid()'))
    name = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    stock = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('ix_product_name', 'name'),
        Index('ix_product_price', 'price'),
    )

def insert_product(session):
    try:
        product = Product(
            name=f"Product_{uuid.uuid4().hex[:8]}",
            price=round(random.uniform(10, 1000), 2),
            stock=random.randint(1, 500)
        )
        session.add(product)
        session.commit()
        logger.info("Insert successful")
    except Exception as e:
        session.rollback()
        logger.error(f"Insert error: {e}")

def select_product(session):
    try:
        product = session.query(Product).first()
        if product:
            logger.info(f"Select successful: {product.name}")
        else:
            logger.info("No product found")
    except Exception as e:
        logger.error(f"Select error: {e}")

def update_product(session):
    try:
        product = session.query(Product).first()
        if product:
            product.price = round(random.uniform(10, 1000), 2)
            session.commit()
            logger.info("Update successful")
        else:
            logger.info("No product found to update")
    except Exception as e:
        session.rollback()
        logger.error(f"Update error: {e}")

def delete_product(session):
    try:
        product = session.query(Product).first()
        if product:
            session.delete(product)
            session.commit()
            logger.info("Delete successful")
        else:
            logger.info("No product found to delete")
    except Exception as e:
        session.rollback()
        logger.error(f"Delete error: {e}")

def main():
    #boto3 session setup , Replace with your own profile name
    session = boto3.Session(profile_name='default')
    # Create a client for the Aurora DSQL, Replace with your own hostname and region
    hostname = "xyxxyxyxyx3rjbyzk7fg5tbydsovpq.dsql.us-east-1.on.aws"
    region = "us-east-1"
    client = session.client("dsql", region_name=region)
    
    # The token expiration time is optional, and the default value 900 seconds
    # Use `generate_db_connect_auth_token` instead if you are not connecting as `admin` user
    password_token = client.generate_db_connect_admin_auth_token(hostname, region, ExpiresIn=86400)

    DB_URL = URL.create("postgresql", username="admin", password=password_token, 
        host=hostname, database="postgres")
    engine = create_engine(DB_URL, pool_size=10, max_overflow=0, connect_args={"sslmode": "require"})
    
    # Create tables outside of a transaction
    with engine.connect() as connection:
        Base.metadata.create_all(bind=connection)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        logger.info("Inserting 1 product")
        insert_product(session)
        logger.info("Selecting 1 product")
        select_product(session)
        logger.info("Updating 1 product")
        update_product(session)
        logger.info("Deleting 1 product")
        delete_product(session)
    except Exception as e:
        session.rollback()
        logger.error(f"Error during testing: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
