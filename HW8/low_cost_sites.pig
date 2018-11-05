-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.

--data = LOAD 'test_ad_data.txt' AS (campaign_id:chararray,
--             date:chararray, time:chararray,
--             keyword:chararray, display_site:chararray, 
--             placement:chararray, was_clicked:int, cpc:int);

-- to load with file patterns
data = LOAD '/dualcore/ad_data[0-9]/part*' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
	     placement:chararray, was_clicked:int, cpc:int);

-- TODO (B): Include only records where was_clicked has a value of 1

data = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field

sites = GROUP data BY display_site;

/* TODO (D): Create a new relation which includes only the 
 *           display site and the total cost of all clicks 
 *           on that site
 */

data = FOREACH sites GENERATE group, SUM(data.cpc) AS cost;

-- TODO (E): Sort that new relation by cost (ascending)

data = ORDER data by cost;

-- TODO (F): Display just the first three records to the screen

data = LIMIT data 4;

DUMP data;
