
-- Create the database tp store the table
create database if not exists liquor_sales;
use liquor_sales;
show databases;

-- Delete the table if it exists
DROP TABLE liquor_data

-- Create the table
CREATE TABLE liquor_data
(
	Category int,
	Item_number int,
	Vendor_number int,
	County_number int,
	Zip_code string,
	Store_number int,
	Date string,
	Pack int,
	Bottle_volume_in_ml int,
	State_bottle_cost double,
	State_bottle_retail double,
	Bottles_sold int,
	Sale_in_dollars double,
	Volume_sold_in_liters double,
	Volume_sold_in_gallons double,
	Invoice_number string,
	Item_number_on_invoice int,
	Store_name string,
	Address string,
	Longitude doube,
	Latitude double,
	City string,
	County string,
	Vendor_name string,
	Item_description string,
	Category_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
tblproperties ("skip.header.line.count" = "1");


LOAD DATA INPATH 'hdfs://ca4022-m/user/adam/csv/cleaned2.csv' OVERWRITE INTO TABLE liquor_data;