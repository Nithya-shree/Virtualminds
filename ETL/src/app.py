# This code helps in creating the HTTP service inorder to reject invalid requests,
# provide stats of the requests.
import logging
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

# Configures logging facility
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initiating flask service
app = Flask(__name__)
# Adex.db is the database that is created once the session is created.
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///../adex.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
app.app_context().push()

# This class defines the structure of the Customer table.
class Customer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    active = db.Column(db.Boolean, default=True)

# This class defines the structure of the IPBlacklist table.
class IPBlacklist(db.Model):
    ip = db.Column(db.String(15), primary_key=True)

# This class defines the structure of the UABlacklist table.
class UABlacklist(db.Model):
    ua = db.Column(db.String(255), primary_key=True)

# This class defines the structure for the Hourlystats(transformed from the requests table) table.
class HourlyStats(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    customer_id = db.Column(db.Integer, db.ForeignKey('customer.id'), nullable=False)
    time = db.Column(db.DateTime, nullable=False)
    request_count = db.Column(db.Integer, default=0)
    invalid_count = db.Column(db.Integer, default=0)

db.create_all()

@app.route("/")
def print_page():
    return "<h1>Data loaded successfully...</h1>"

# Post method to receive the request and identify if the request valid,
# is missing fields, incorrect Customer_id, is blacklisted IP or Useragents.
@app.route('/receive', methods=['POST'])
def receive_request():
    data = request.get_json()
    if not data:
        logger.error("Malformed JSON received")
        # Incorrect json
        return jsonify({"error": "Malformed JSON"}), 400
    # Validate the fields
    required_fields = {"customerID", "tagID", "userID", "remoteIP", "timestamp"}
    if not required_fields.issubset(data.keys()):
        logger.error("Missing fields in the request: %s", data)
        return jsonify({"error": "Missing fields"}), 400
    # Validate if the Cutomer ID is available
    customer = db.session.get(Customer, data['customerID'])
    if not customer or not customer.active:
        logger.error("Invalid customer ID or inactive customer: %s", data['customerID'])
        return jsonify({"error": "Invalid customer ID"}), 400
    # Validates for the blacklisted IP
    if db.session.get(IPBlacklist, data['remoteIP']):
        logger.error("IP is blacklisted: %s", data['remoteIP'])
        return jsonify({"error": "IP is blacklisted"}), 400
    # Validates for the blacklisted User Agents
    user_agent = request.headers.get('User-Agent')
    if db.session.get(UABlacklist, user_agent):
        logger.error("User agent is blacklisted: %s", user_agent)
        return jsonify({"error": "User agent is blacklisted"}), 400

    timestamp = datetime.fromtimestamp(data['timestamp'])
    start_of_hour = timestamp.replace(minute=0, second=0, microsecond=0)

    stats = HourlyStats.query.filter_by(customer_id=data['customerID'], time=start_of_hour).first()
    if not stats:
        stats = HourlyStats(customer_id=data['customerID'], time=start_of_hour)
        db.session.add(stats)

    stats.request_count += 1
    db.session.commit()

    logger.info("Request accepted for customer ID %s at %s", data['customerID'], start_of_hour)
    return jsonify({"message": "Request accepted"}), 200
# This GET method will provide the stats for the customer ID
# and the day that is provided in the request.
@app.route('/stats/<int:customer_id>/<string:day>', methods=['GET'])
def get_stats(customer_id, day):
    customer = db.session.get(Customer, customer_id)
    if not customer:
        logger.error("Customer not found: %s", customer_id)
        return jsonify({"error": "Customer not found"}), 404

    try:
        start_of_day = datetime.strptime(day, '%Y-%m-%d')
        end_of_day = start_of_day.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Invalid date format: %s", day)
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    stats = HourlyStats.query.filter(
        HourlyStats.customer_id == customer_id,
        HourlyStats.time >= start_of_day,
        HourlyStats.time <= end_of_day
    ).all()

    if not stats:
        logger.info("No requests found for customer ID %s on %s", customer_id, day)
        return jsonify({
            "customer_id": customer_id,
            "date": day,
            "total_requests": 0,
            "hourly_stats": []
        }), 200

    response = {
        "customer_id": customer_id,
        "date": day,
        "total_requests": sum(stat.request_count for stat in stats),
        "hourly_stats": [
            {"hour": stat.time.hour, "request_count": stat.request_count, "invalid_count": stat.invalid_count}
            for stat in stats
        ]
    }

    logger.info("Statistics retrieved for customer ID %s on %s", customer_id, day)
    return jsonify(response), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
