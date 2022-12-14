--# 
--# Date : 10-December-2022
--# @mano 
--# References 
--# https://medium.com/snowflake/hey-snowflake-send-me-an-email-243741a0fe3

use role accountadmin ;
SET emailAddress = 'manoj1.madhavan@outlook.com';

use role accountadmin ;

    
-- # For CURRENT_TIMESTAMP() timestamp formatting
ALTER SESSION SET timestamp_output_format = 'YYYY-MM-DD HH24:MI:SS'; 



	create or replace notification integration email_notification_int
    type=email
    enabled=true
    allowed_recipients=($emailAddress)
	;

	grant usage on integration email_notification_int to role accountadmin;







with pretty_email_results as procedure()
returns string
language python
packages = ('snowflake-snowpark-python', 'tabulate')
handler = 'x'
as
$$
def x(session):
    printed = session.sql(
        "select * from table(result_scan(last_query_id(-1)))"
      ).to_pandas().to_markdown()
    session.call('system$send_email',
        'email_notification_int',
         'manoj1.madhavan@outlook.com',
        'Email Alert: Task A has finished.',
        printed)
$$
call pretty_email_results();