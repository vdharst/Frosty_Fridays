-- Environment Configuration
USE database FROSTY_DB;
USE warehouse COMPUTE_WH;

-- Create schema
CREATE OR REPLACE schema week_2;
USE schema week_2;

-- Create PARQUET file format
CREATE OR REPLACE file format week_2_parquet_ffmt
type = 'parquet';
    
-- Create internal stage 
CREATE OR REPLACE stage week_2_parquet_stg
url='s3://frostyfridaychallenges/challenge_2';

-- list files
LIST @week_2_parquet_stg;

-- check column definitions for .parquet with INFER_SCHEMA
SELECT *
FROM TABLE (
infer_schema(
location =>'@week_2_parquet_stg/',
file_format =>'week_2_parquet_ffmt')
);
            
-- Inspect contents of .parquet
SELECT *
FROM @week_2_parquet_stg (
file_format => 'week_2_parquet_ffmt',
pattern => 'challenge_2/employees.parquet');

--get table metadata (clumn names and data types per column)
SELECT generate_column_description(array_agg(object_construct(*)), 'table') as column_desc
FROM TABLE (
infer_schema(
location=>'@week_2_parquet_stg/',
file_format=>'week_2_parquet_ffmt')
);

--create table week_2_parquet_tbl using template
CREATE OR REPLACE TABLE week_2_parquet_tbl USING template (
SELECT array_agg(object_construct(*)) 
FROM table (
infer_schema(
location=>'@week_2_parquet_stg/',
file_format=>'week_2_parquet_ffmt')
));

-- Have a look at table
select * from week_2_parquet_tbl;

-- Load data into table from stage
-- will only load columns which are present in tbl
COPY INTO week_2_parquet_tbl
FROM @week_2_parquet_stg/employees.parquet
file_format = 'week_2_parquet_ffmt',
ON_ERROR = 'ABORT_STATEMENT' 
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = FALSE; --will not remove file after loading into tbl

-- Have a look at table again
select * from week_2_parquet_tbl;

-- create a view
CREATE OR REPLACE VIEW week_2_parquet_view as 
 SELECT
 "employee_id",
 "job_title",
 "dept"
 FROM week_2_parquet_tbl;
 
-- create stream
Create or Replace stream week_2_stream
on view week_2_parquet_view;

-- do updates as given by FF
UPDATE week_2_parquet_tbl SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE week_2_parquet_tbl SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE week_2_parquet_tbl SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE week_2_parquet_tbl SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE week_2_parquet_tbl SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

-- check stream for content
select * from week_2_stream;

--learned a lot, thanks team FF and thanks Atzmon for your notes.
    
