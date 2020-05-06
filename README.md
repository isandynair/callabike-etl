# Callabike-ETL
ETL Pipeline for Call a Bike dataset

#### Follow the instructions below and download the csv files from Deutsche-Bahn's website, inside the data folder

```
mkdir data
wget http://download-data.deutschebahn.com/static/datasets/callabike/20170516/OPENDATA_BOOKING_CALL_A_BIKE.zip && unzip OPENDATA_BOOKING_CALL_A_BIKE.zip -d data/ && rm OPENDATA_BOOKING_CALL_A_BIKE.zip
wget http://download-data.deutschebahn.com/static/datasets/callabike/20170516/OPENDATA_VEHICLE_CALL_A_BIKE.zip && unzip OPENDATA_VEHICLE_CALL_A_BIKE.zip -d data/ && rm OPENDATA_VEHICLE_CALL_A_BIKE.zip
wget http://download-data.deutschebahn.com/static/datasets/callabike/20170516/OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip && unzip OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip -d data/ && rm OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip
wget http://download-data.deutschebahn.com/static/datasets/callabike/20170516/OPENDATA_CATEGORY_CALL_A_BIKE.zip && unzip OPENDATA_CATEGORY_CALL_A_BIKE.zip -d data/ && rm OPENDATA_CATEGORY_CALL_A_BIKE.zip
wget http://download-data.deutschebahn.com/static/datasets/callabike/20170516/OPENDATA_CATEGORY_CALL_A_BIKE.zip && unzip OPENDATA_CATEGORY_CALL_A_BIKE.zip -d data/ && rm OPENDATA_CATEGORY_CALL_A_BIKE.zip
```

#### Install the requirements

```
pip install -r requirements.txt
```

#### Run the etl.py file

```
python etl.py
```

This would take some time depending on the system, meanwhile have a coffee!

You will find the callabike.csv output file in the same directory!
