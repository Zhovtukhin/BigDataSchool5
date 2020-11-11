### 3. создать своего юзера + схему
``` (sql)
REATE SCHEMA zhovtukhin_schema  AUTHORIZATION zhovtukhin  

CREATE DATABASE SCOPED CREDENTIAL zhovtukhin_credential2
WITH IDENTITY = 'zhovtukhin'
     , SECRET = 'Qy/AA1sMuYB+OxZc6zIt68+Bb6+EAX7HFh7utjCWTBOWXMagJGJnx9/w51I7hnepqFXY8FYAQmWTxZX6tuaelA==' 
;
``` 

### 5 создать екстернал data source
``` (sql)
CREATE EXTERNAL DATA SOURCE zhovtukhin_source0
WITH
  ( LOCATION = 'wasbs://seccont@bigdata52.blob.core.windows.net' ,
    CREDENTIAL = zhovtukhin_credential2,
    TYPE = HADOOP
  ) ;
```

###  6 Создать екстернал таблицу для файла  yellow_tripdata_2020-01 на основе екстернал data source
``` (sql)
CREATE EXTERNAL FILE FORMAT zhovtukhin_format
WITH (  
    FORMAT_TYPE = DELIMITEDTEXT , 
	FORMAT_OPTIONS (  
        FIELD_TERMINATOR = ',',  
        DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss',
		Encoding = 'UTF8', 
		USE_TYPE_DEFAULT = FALSE,
		first_row=2)
);


CREATE EXTERNAL TABLE zhovtukhin_schema.yellow_trip_external0
 (
 VendorID                 int NULL,
	tpep_pickup_datetime     datetime NOT NULL,
	tpep_dropoff_datetime    datetime NULL,
	passenger_count          int NULL,
	trip_distance            real NULL,
	RatecodeID               int NULL,
	store_and_fwd_flag       char(1) NULL,
	PULocationID             int NULL,
	DOLocationID             int NULL,
	payment_type             int NULL,
	fare_amount              real NULL,
	extra                    real NULL,
	mta_tax                  real NULL,
	tip_amount               real NULL,
	tolls_amount             real NULL,
	improvement_surcharge    real NULL,
	total_amount             real NULL,
	congestion_surcharge     real NULL
)
WITH (
        LOCATION='/yellow_tripdata_2020-01.csv',
        DATA_SOURCE = zhovtukhin_source0,
        FILE_FORMAT = zhovtukhin_format,
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
    );
``` 

### 7 Выгрузить данные из external table в таблицу "ВашаСхема".fact_tripdata.  Таблица должна быть Hash-distributed tables
``` (sql)
CREATE TABLE zhovtukhin_schema.fact_tripdata
WITH (DISTRIBUTION = HASH(tpep_pickup_datetime) )
AS SELECT * FROM
zhovtukhin_schema.yellow_trip_external0;
```


### 8 Создать таблицы справочники на основе документа 
``` (sql)
CREATE TABLE zhovtukhin_schema.Vendor
(  id int NOT NULL,  
   Name varchar(50))  
WITH  (DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	);

 
 CREATE TABLE zhovtukhin_schema.RateCode
(  id int NOT NULL,  
   Name varchar(50))  
WITH (DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	);

CREATE TABLE zhovtukhin_schema.Payment_type
(  id int NOT NULL,  
   Name varchar(50))  
WITH (DISTRIBUTION = REPLICATE 
	 ,CLUSTERED COLUMNSTORE INDEX
	);
```
