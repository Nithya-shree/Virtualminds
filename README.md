# Virtualminds Coding Task
This Repository contains solution which generates the Customer and other data. It filters out the invalid requests that are either missing the required fields, blacklisted IP addresses. The solution also calculates the statistics per customer per hour ans stores in the DB. 

### Sample data used:
Generated the sample data using python defining the table structure and the number of records in the code.

### Tools used:
* Python(Pandas)
* SQLAlchemy to connect to SQLiteDB
* Flask
* Postman

### Source code structure
  #### ETL/src
  | Path | Description |
  | --- | --- |
  | [main.py](https://github.com/Nithya-shree/Virtualminds/blob/main/ETL/src/main.py) |  Main pipeline script incorporating the database initialization and app creation. |
  | [init_db.py](https://github.com/Nithya-shree/Virtualminds/blob/main/ETL/src/init_db.py) |  Handling the connection to database with a defined structure. |
  | [app.py](https://github.com/Nithya-shree/Virtualminds/blob/main/ETL/src/app.py) |  Implementation of Flask API to request the stats. |

  #### ETL/data
  * customers.csv
  * ip_blacklist.csv
  * ua_blacklist.csv
  * requests.csv

  #### Data_generate/
  | Path | Description |
  | --- | --- |
  | [main.py](https://github.com/Nithya-shree/Virtualminds/blob/main/Data_generate/main.py) |  Main pipeline script to generate sample data |

### Steps to initiate the process
* Clone the repository
* To create new dataset execute ``` python main.py ``` in Data_generate folder.
* To execute the solution in ETL/src, run the ``` python main.py``` which initiates the database connection and also the API providing the HTTP URL to connect in POSTMAN.
* Open Postman in browser and make the below configurations:
    #### Select POST from drop-down:
      URL: http://127.0.0.1:8000/receive
      Content-Type: application/json
      Key: User-Agent: Mozilla/5.0
    * Example JSON:
      ```JSON
      {
        "customerID": 514,
  	    "tagID": 9,
        "userID": "1898c0ef-c15b-4269-bdea-cd1602239125",
        "remoteIP": "63.100.44.170",
        "timestamp": 1687390588
      }
      ``` 
  
<img src="https://github.com/Nithya-shree/Virtualminds/blob/main/Screenshots/post_method.png" />

   #### Select GET from drop-down:
      URL: http://127.0.0.1:8000/stats/514/2023-06-22
      
<img src="https://github.com/Nithya-shree/Virtualminds/blob/main/Screenshots/get_method.png" />

### Validation samples:
  #### Blacklisted IP:

  <img src="https://github.com/Nithya-shree/Virtualminds/blob/main/Screenshots/post_ip_blacklist.png" />

  #### Invalid Customer_ID:

  <img src="https://github.com/Nithya-shree/Virtualminds/blob/main/Screenshots/post_invalid_CID.png" />

      
  

  
