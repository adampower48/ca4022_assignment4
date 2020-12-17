
-- Copy data file to my local folder
cp /data/Iowa_Liquor_Sales.csv /home/Owner


-- Read in the data
read_liquor_data = LOAD 'Iowa_Liquor_Sales.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'Unix', 'SKIP_INPUT_HEADER') AS (
			Invoice_nd_item_number: chararray,
			Date: chararray,
			Store_number: chararray,
			Store_name: chararray,
			Address: chararray,
			City: chararray,
			Zip_code: chararray,
			Store_location: chararray,
			County_number: chararray,
			County: chararray,
			Category: chararray,
			Category_name: chararray,
			Vendor_number: chararray,
			Vendor_name: chararray,
			Item_number: chararray,
			Item_description: chararray,
			Pack: chararray,
			Bottle_volume_in_ml: chararray,
			State_bottle_cost: chararray,
			State_bottle_retail: chararray,
			Bottles_sold: chararray,
			Sale_in_dollars: chararray,
			Volume_sold_in_liters: chararray,
			Volume_sold_in_gallons: chararray);


-- Limit the number of rows
--limited_read_liqour_data = LIMIT read_liquor_data 10;

-- Output these rows
--dump limited_read_liqour_data


-- Remove the newline characters and commas from the store location
-- Seperate out the invoice number from the item number
-- Remove the dollar signs ($) from the money columns
liquor_data = FOREACH read_liquor_data GENERATE
	SUBSTRING(Invoice_nd_item_number, 0, 6) AS Invoice_number, -- seperate out invoice num from invoice_nd_item_num
	SUBSTRING(Invoice_nd_item_number, 6, 12) AS Item_number_on_invoice, -- seperate out item num from invoice_nd_item_num
	Date,
	Store_number,
	REPLACE(REPLACE(Store_name,'\n',' '),',','') AS Store_name,
	REPLACE(REPLACE(Address,'\n',' '),',','') AS Address,
	City,
	Zip_code,
	REPLACE(REPLACE(Store_location,'\n',' '),',','') AS Store_location, -- remove newlines and commas
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
	REPLACE(State_bottle_cost,'\\\$','') AS State_bottle_cost, -- turn to acceptable format for float - remove $
	REPLACE(State_bottle_retail,'\\\$','') AS State_bottle_retail, -- turn to acceptable format for float - remove $
	Bottles_sold,
	REPLACE(Sale_in_dollars,'\\\$','') AS Sale_in_dollars, -- turn to acceptable format for float - remove $
	Volume_sold_in_liters,
	Volume_sold_in_gallons;


-- Limit the number of rows
--limited_liqour_data = LIMIT liquor_data 10;

-- Output these rows
--dump limited_liqour_data


-- Extract the longitude and latitude values out from the store location column
cleaned_liquor_data = FOREACH liquor_data GENERATE
	Invoice_number,
	Item_number_on_invoice,
	Date,
	Store_number,
	Store_name,
	Address,
	City,
	Zip_code,
	-- Dont include Store_location as it just contains all the data from the other address columns joined into one
	REGEX_EXTRACT(Store_location, '\\((-*[\\d]+(.[\\d]+)*)', 1) AS Longitude, -- extract longitude from location, pattern for this '\(-*[\d]+.[\d]+'
	REGEX_EXTRACT(Store_location, '(-*[\\d]+(.[\\d]+)*)\\)', 1) AS Latitude, -- extract latitude from location, pattern for this '-*[\d]+.[\d]+\)'
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
--limited_cleaned_liqour_data = LIMIT cleaned_liquor_data 10;

-- Output these rows
--dump limited_cleaned_liqour_data


-- Filter to one row that is causing problems
--problem = FILTER cleaned_liquor_data BY Zip_code == '50021';
--dump problem


-- Output the cleaned file as a CSV
STORE cleaned_liquor_data INTO 'Cleaned_Iowa_Liquor_Sales.csv' using PigStorage(',');
-- Took 9 minutes
