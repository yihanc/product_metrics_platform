// Create Query
CREATE TABLE dim_tags (_Count BIGINT, _ExcerptPostId BIGINT, _Id BIGINT, _TagName VARCHAR, _WikiPostId BIGINT) WITH ( format = 'parquet', EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/tags.parquet' );


CREATE TABLE dim_users (_Id, BIGINT, _AboutMe VARCHAR, _AccountId, BIGINT, _CreationDate VARCHAR, _DisplayName VARCHAR, _DownVotes BIGINT, _LastAccessDate VARCHAR, _Location VARCHAR, _ProfileImageUrl VARCHAR, _Reputation BIGINT, _UpVotes BIGINT, _View BIGINTs, _WebsiteUrl VARCHAR) WITH ( format = 'parquet', EXTERNAL_LOCATION ='s3://stackoverflow-ds/raw/users.parquet' );
