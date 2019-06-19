-- Presto CLI: $ presto-cli --catalog hive --schema web
-- Create Query

CREATE TABLE dim_tags (
  cnt INT, 
  excerpt_post_id INT, 
  id INT, 
  tag_name VARCHAR, 
  wiki_post_id INT
) WITH ( 
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/tags.parquet' 
);

CREATE TABLE dim_users (
  about_me VARCHAR, 
  account_id INT, 
  creation_date VARCHAR, 
  display_name VARCHAR, 
  down_votes INT, 
  id INT, 
  last_access_date VARCHAR, 
  location VARCHAR, 
  profile_image_url VARCHAR, 
  reputation INT, 
  up_votes INT, 
  view INT, 
  website_url VARCHAR
) WITH ( 
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/users.parquet' 
);


