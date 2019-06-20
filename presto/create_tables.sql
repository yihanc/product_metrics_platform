/* 
Notes:

Presto CLI: $ presto-cli --catalog hive --schema web

Parquet Tool to read schema:
$ wget http://central.maven.org/maven2/org/apache/parquet/parquet-tools/1.9.0/parquet-tools-1.9.0.jar
$ hadoop jar parquet-tools-1.9.0.jar schema s3://xxx
*/

-- Create Query

CREATE TABLE IF NOT EXISTS dim_tags (
  cnt INT, 
  excerpt_post_id INT, 
  id INT, 
  tag_name VARCHAR, 
  wiki_post_id INT
) WITH ( 
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/tags.parquet' 
);

--

CREATE TABLE IF NOT EXISTS dim_users (
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

--

CREATE TABLE IF NOT EXISTS dim_comments ( 
  creation_date VARCHAR, 
  id BIGINT, 
  post_id BIGINT, 
  score BIGINT, 
  text VARCHAR, 
  user_display_name VARCHAR, 
  user_id BIGINT, 
  load_date VARCHAR 
) WITH ( 
  -- PARTITIONED_BY = ARRAY['load_date'],  # Keep creating NULL value
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/2019-06-19/comments.parquet'
);

--

CREATE TABLE IF NOT EXISTS t10 (
  accepted_answer_id BIGINT,
  answer_count BIGINT,
  body VARCHAR,
  closed_date VARCHAR,
  comment_count BIGINT,
  community_owned_date VARCHAR,
  creation_date VARCHAR,
  favorite_count BIGINT,
  id BIGINT,
  last_activity_date VARCHAR,
  last_edit_date VARCHAR,
  last_editor_display_name VARCHAR,    
  last_editor_user_id BIGINT,  
  owner_display_name VARCHAR,
  owner_user_id BIGINT,
  parent_id BIGINT,
  post_type_id BIGINT,
  score BIGINT,
  tags VARCHAR,
  title VARCHAR,
  view_count BIGINT
) WITH ( 
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION = REPLACE('s3://stackoverflow-ds/raw/<DATE>/posts.parquet', '<DATE>', CAST(current_date AS VARCHAR))
);

--

CREATE TABLE IF NOT EXISTS fct_posthistory (
  id INT,
  post_id INT,
  creation_date VARCHAR COMMENT "creation date w/ timestamp in UTC format"
  user_id INT,
  text VARCHAR,
  revision_guid VARCHAR,
  create_date VARCHAR COMMENT "Date only in YYYY-MM-DD format. Partition Only",
  post_history_type_id INT
) WITH ( 
  PARTITIONED_BY = ARRAY['create_date', 'post_type_id'],
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION = REPLACE('s3://stackoverflow-ds/raw/<DATE>/post_history.parquet', '<DATE>', CAST(current_date AS VARCHAR))
);
