-- Load only the ad_data1 and ad_data2 directories
data = LOAD '/dualcore/ad_data[12]' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);

grouped = GROUP data BY display_site;

by_site = FOREACH grouped {
  -- Include only records where the ad was clicked
	clicked = FILTER data BY was_clicked==1;
	clicked = COUNT(clicked);
	
  -- count the number of records in this group

	total = COUNT(data);
	GENERATE group, (clicked*1000000/total) AS ctr;
  /* Calculate the click-through rate by dividing the 
   * clicked ads in this group by the total number of ads
   * in this group.
   */
}

-- sort the records in ascending order of clickthrough rate
data = ORDER by_site BY ctr ASC;

-- show just the first three

data = LIMIT data 3;

DUMP data;
