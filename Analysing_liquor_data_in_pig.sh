

-- Read in the cleaned data
read_in_liquor_data = LOAD 'hdfs://ca4022-m/user/adam/csv/cleaned2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'Unix', 'SKIP_INPUT_HEADER') AS (
	Category: int,
	Item_number: int,
	Vendor_number: int,
	County_number: int,
	Zip_code: chararray,
	Store_number: int,
	Date: chararray,
	Pack: int,
	Bottle_volume_in_ml: int,
	State_bottle_cost: float,
	State_bottle_retail: float,
	Bottles_sold: int,
	Sale_in_dollars: float,
	Volume_sold_in_liters: float,
	Volume_sold_in_gallons: float,
	Invoice_number: chararray,
	Item_number_on_invoice: int,
	Store_name: chararray,
	Address: chararray,
	Longitude: float,
	Latitude: float,
	City: chararray,
	County: chararray,
	Vendor_name: chararray,
	Item_description: chararray,
	Category_name: chararray);

-- Re-order columns
liquor_data = FOREACH read_in_liquor_data GENERATE
	Invoice_number,
	Item_number_on_invoice,
	Date,
	Store_number,
	Store_name,
	Address,
	City,
	Zip_code,
	Longitude,
	Latitude,
	County_number,
	County,
	Category,
	Category_name,
	Vendor_number,
	Vendor_name,
	Item_number,
	Item_description,
	Pack,
	Bottle_volume_in_ml,
	State_bottle_cost,
	State_bottle_retail,
	Bottles_sold,
	Sale_in_dollars,
	Volume_sold_in_liters,
	Volume_sold_in_gallons;


-- Limit the number of rows
limited_liqour_data = LIMIT liquor_data 10;

-- Output these rows
dump limited_liqour_data


-- Count unique invoice numbers
invoices = FOREACH liquor_data GENERATE Invoice_number;
unique_invoices = DISTINCT invoices;
grouped_invoices = GROUP unique_invoices ALL;
invoice_count = FOREACH grouped_invoices GENERATE COUNT(unique_invoices) AS num_invoices;
dump invoice_count


-- Count number of counties
counties = FOREACH liquor_data GENERATE County_number;
unique_counties = DISTINCT counties;
grouped_counties = GROUP unique_counties ALL;
county_count = FOREACH grouped_counties GENERATE COUNT(unique_counties) AS num_counties;
dump county_count


-- Which county consumes the most alcohol
county_and_volume_sold = FOREACH liquor_data GENERATE County_number, County, Volume_sold_in_liters;
grouped_volumes_by_county = GROUP county_and_volume_sold BY (County_number, County);
volume_sold_by_county = FOREACH grouped_volumes_by_county GENERATE group.County_number, group.County, ROUND_TO(SUM(county_and_volume_sold.Volume_sold_in_liters), 2) AS total_volume;
ordered_counties_volume = ORDER volume_sold_by_county BY total_volume DESC;
top_10_counties_volume = LIMIT ordered_counties_volume 10;
dump top_10_counties_volume


-- Which county spends the most on alcohol
county_and_amount_spent = FOREACH liquor_data GENERATE County_number, County, Sale_in_dollars;
grouped_sales_by_county = GROUP county_and_amount_spent BY (County_number, County);
amount_spent_by_county = FOREACH grouped_sales_by_county GENERATE group.County_number, group.County, ROUND_TO(SUM(county_and_amount_spent.Sale_in_dollars), 2) AS total_sales;
ordered_counties_spending = ORDER amount_spent_by_county BY total_sales DESC;
top_10_counties_spending = LIMIT ordered_counties_spending 10;
dump top_10_counties_spending


-- Average volume & price of orders
all_invoice_volume_and_price = FOREACH liquor_data GENERATE Invoice_number, Volume_sold_in_liters, Sale_in_dollars;
grouped_invoices = GROUP all_invoice_volume_and_price BY (Invoice_number);
summed_invoice_volume_and_price = FOREACH grouped_invoices GENERATE group AS Invoice_number, ROUND_TO(SUM(all_invoice_volume_and_price.Volume_sold_in_liters), 2) AS order_volume, ROUND_TO(SUM(all_invoice_volume_and_price.Sale_in_dollars), 2) AS order_price;
grouped_invoice_volume_and_price = GROUP summed_invoice_volume_and_price ALL;

average_order_vol = FOREACH grouped_invoice_volume_and_price GENERATE ROUND_TO(AVG(summed_invoice_volume_and_price.order_volume), 2) AS avg_volume;
dump average_order_vol

average_order_price = FOREACH grouped_invoice_volume_and_price GENERATE ROUND_TO(AVG(summed_invoice_volume_and_price.order_price), 2) AS avg_price;
dump average_order_price
