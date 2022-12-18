--Create Stage
create stage frosty_db.challenges.stage_week1
url = 's3://frostyfridaychallenges/challenge_1/';

--List stage
List @stage_week1;

--Create File Format
create file format Variant_Format
TYPE='CSV',FIELD_DELIMITER=none;

--Check contents of csv and what is from which CSV
select $1,METADATA$FILENAME from @stage_week1 (file_format=>'VARIANT_FORMAT');

--Create table for csv contents
create or replace table CSVWeek1 (
KOLOM1 TEXT,
FROM_FILE TEXT);

--Copy into table
COPY INTO csvweek1
FROM 
(SELECT $1, METADATA$FILENAME FROM @stage_week1)
FILE_FORMAT='VARIANT_FORMAT'
;

--Check resuls of copy into
Select * from csvweek1;
