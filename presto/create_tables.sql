-- Presto CLI: $ presto-cli --catalog hive --schema web
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

CREATE TABLE IF NOT EXISTS dim_comments (
  creation_date VARCHAR,
  id VARCHAR,
  post_id INT,
  score INT,
  text INT,
  user_display_name VARCHAR,
  user_id INT,
  load_date VARCHAR
) WITH ( 
  PARTITIONED_BY = ARRAY['load_date'],
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/comments.parquet' 
);

CREATE TABLE IF NOT EXISTS dim_posts (
  accepted_answer_id INT,
  answer_count INT,
  body VARCHAR,
  comment_count INT,
  community_owned_date VARCHAR,
  creation_date VARCHAR,
  favorite_count INT,
  id VARCHAR,
  last_activity_date VARCHAR,
  last_editor_display_name VARCHAR,  
  last_editor_user_id INT,
  last_edit_date VARCHAR,
  owner_user_id INT,
  score INT,
  tags VARCHAR,
  title VARCHAR,
  view_count INT,
  load_date VARCHAR,
  post_type_id INT
) WITH ( 
  PARTITIONED_BY = ARRAY['load_date', 'post_type_id'],
  FORMAT = 'parquet', 
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/posts.parquet' 
);


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
  EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/posthistory.parquet' 
);
