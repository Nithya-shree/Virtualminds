import subprocess
import sys

def initialize_database():
    subprocess.run([sys.executable, 'init_db.py'])

def run_application():
    subprocess.run([sys.executable, 'app.py'])

if __name__ == '__main__':
    # Initialize the database
    print("Initializing the database...")
    initialize_database()

    # Run the Flask application
    print("Starting the Flask application...")
    run_application()
