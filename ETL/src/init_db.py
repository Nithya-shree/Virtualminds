# This code reads the data that is generated,
# removes the duplicates and loads the data into the SQLAlchemy DB
import pandas as pd
from app import db, Customer, IPBlacklist, UABlacklist, HourlyStats, app
from datetime import datetime
import os
import logging

# Configures logging facility
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This function removes the duplicates.
# @param: file_path of the file, key_column for the table
def remove_duplicates(file_path, key_column):
    df = pd.read_csv(file_path)
    df.drop_duplicates(subset=[key_column], keep='first', inplace=True)
    df.to_csv(file_path, index=False)
    logger.info(f"Removed duplicates from {file_path}")

# This function loads the customer data.
# @param: file_path of the file
def load_customers(file_path):
    df = pd.read_csv(file_path)
    customers = []
    for _, row in df.iterrows():
        if not db.session.get(Customer, row['id']):
            customer = Customer(id=row['id'], name=row['name'], active=bool(row['active']))
            customers.append(customer)
    db.session.bulk_save_objects(customers)
    db.session.commit()
    logger.info("Customers loaded successfully")

# This function loads the blacklisted IP addresses.
# @param: file_path of the file
def load_ip_blacklist(file_path):
    df = pd.read_csv(file_path)
    ip_blacklist = []
    for _, row in df.iterrows():
        if not db.session.get(IPBlacklist, row['ip']):
            ip_blacklist.append(IPBlacklist(ip=row['ip']))
    db.session.bulk_save_objects(ip_blacklist)
    db.session.commit()
    logger.info("IP blacklist loaded successfully")

# This function loads the blacklisted User agents.
# @param: file_path of the file
def load_ua_blacklist(file_path):
    df = pd.read_csv(file_path)
    ua_blacklist = []
    for _, row in df.iterrows():
        if not db.session.get(UABlacklist, row['ua']):
            ua_blacklist.append(UABlacklist(ua=row['ua']))
    db.session.bulk_save_objects(ua_blacklist)
    db.session.commit()
    logger.info("UA blacklist loaded successfully")

# This function loads the requests received and collect the stats.
# @param: file_path of the file
def load_requests(file_path):
    df = pd.read_csv(file_path)
    request_dict = {}

    for _, row in df.iterrows():
        timestamp = datetime.fromtimestamp(row['timestamp'])
        start_of_hour = timestamp.replace(minute=0, second=0, microsecond=0)
        key = (row['customerID'], start_of_hour)

        if key not in request_dict:
            request_dict[key] = 0
        request_dict[key] += 1

    # Batch load HourlyStats data
    hourly_stats = {}
    for (customer_id, start_of_hour), count in request_dict.items():
        stats = HourlyStats.query.filter_by(customer_id=customer_id, time=start_of_hour).first()
        if not stats:
            stats = HourlyStats(customer_id=customer_id, time=start_of_hour, request_count=0, invalid_count=0)
            db.session.add(stats)
        if (customer_id, start_of_hour) not in hourly_stats:
            hourly_stats[(customer_id, start_of_hour)] = stats
        hourly_stats[(customer_id, start_of_hour)].request_count += count

    db.session.bulk_save_objects(hourly_stats.values())
    db.session.commit()
    logger.info("Requests loaded successfully")

# This function initialises the database for the data to be loaded.
# @param: file_path of the file
def initialize_database():
    data_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

    # Remove duplicates from CSV files
    logger.info("Removing duplicates from CSV files")
    remove_duplicates(os.path.join(data_folder, 'customers.csv'), 'id')
    remove_duplicates(os.path.join(data_folder, 'ip_blacklist.csv'), 'ip')
    remove_duplicates(os.path.join(data_folder, 'ua_blacklist.csv'), 'ua')

    # Load data into database
    logger.info("Loading data into database")
    load_customers(os.path.join(data_folder, 'customers.csv'))
    load_ip_blacklist(os.path.join(data_folder, 'ip_blacklist.csv'))
    load_ua_blacklist(os.path.join(data_folder, 'ua_blacklist.csv'))
    load_requests(os.path.join(data_folder, 'requests.csv'))

if __name__ == '__main__':
    with app.app_context():
        logger.info("Creating tables and loading initial data...")
        db.create_all()
        initialize_database()
        logger.info("Database initialization complete")
