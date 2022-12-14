use role accountadmin ;
use database demo_db ;
use warehouse demo_wh ;




--Show the active integrations defined.
show integrations ;

--Define the integration. 
create or replace api integration MY_LAMBDA
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::537269197662:role/SFLambdaIAMRole'
  api_allowed_prefixes = ('https://cc3czsleb4.execute-api.eu-west-1.amazonaws.com/test/sf-proxy/')
  enabled = true

--Describe the Integration  - Keep a look on the details as this is required to setup the controls on the API Gateway.
describe integration MY_LAMBDA ;

--Defining the External Function with the reference to the API endpoint. 
create external function translate_eng_italian(message string)
    returns variant
    api_integration = MY_LAMBDA
    as 'https://cc3czsleb4.execute-api.eu-west-1.amazonaws.com/test/sf-proxy/'
    ;


show external functions ;
grant usage on function TRANSLATE_ENG_ITALIAN(string) to accountadmin ;


describe function TRANSLATE_ENG_ITALIAN (string) ;
use schema public ;

select msg, TRANSLATE_ENG_ITALIAN(msg) from messages;
select msg from messages;

select * from messages ;



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