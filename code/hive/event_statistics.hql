-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory 
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
-- 
-- To do this, run the following commands for events.csv, 
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R root /input
-- 4. exit 
-- 5. hdfs dfs -put /path-to-events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be 
-- '/input/mortality'
-- ***************************************************************************
-- create events table 
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/events';

-- create mortality events table 
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/mortality';

-- ******************************************************
-- Task 1:
-- By manipulating the above two tables, 
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients
DROP VIEW IF EXISTS alive_events;
CREATE VIEW alive_events 
AS
SELECT e.patient_id, e.event_id, e.time 
FROM events e LEFT JOIN mortality m ON e.patient_id = m.patient_id
where m.patient_id is NULL;




-- find events for dead patients
DROP VIEW IF EXISTS dead_events;
CREATE VIEW dead_events 
AS
SELECT e.patient_id, e.event_id, e.time 
FROM events e LEFT JOIN mortality m ON e.patient_id = m.patient_id
where m.patient_id is not NULL;





-- ************************************************
-- Task 2: Event count metrics
-- Compute average, min and max of event counts 
-- for alive and dead patients respectively  
-- ************************************************
-- alive patients
SELECT avg(s.event_count), min(s.event_count), max(s.event_count)
FROM (SELECT de.patient_id, count(de.event_id) event_count FROM alive_events de GROUP BY patient_id) s;

-- dead patients
SELECT avg(s.event_count), min(s.event_count), max(s.event_count)
FROM (SELECT de.patient_id, count(de.event_id) event_count FROM dead_events de GROUP BY patient_id) s;





-- ************************************************
-- Task 3: Encounter count metrics 
-- Compute average, min and max of encounter counts 
-- for alive and dead patients respectively
-- ************************************************
-- alive
SELECT avg(s.encounter_count), min(s.encounter_count), max(s.encounter_count)
FROM (SELECT ae.patient_id, ae.time event_count FROM alive_events ae GROUP BY ae.patient_id, ae.time) s;





-- dead
SELECT avg(s.encounter_count), min(s.encounter_count), max(s.encounter_count)
FROM (SELECT de.patient_id, de.time event_count FROM alive_events de GROUP BY de.patient_id, de.time) s;






-- ************************************************
-- Task 4: Record length metrics
-- Compute average, median, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************
-- alive 
SELECT avg(record_length), percentile(record_length, 0.5), min(record_length), max(record_length)
FROM (SELECT ae.patient_id, max(ae.time)  - min(ae.time) record_length FROM alive_events ae GROUP BY ae.patient_id) s;





-- dead
SELECT avg(record_length), percentile(record_length, 0.5), min(record_length), max(record_length)
FROM (SELECT de.patient_id, max(de.time) - min(ae.time) record_length FROM alive_events de GROUP BY de.patient_id) s;





-- ******************************************* 
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM alive_events
-- ***** your code below *****


---- lab
SELECT event_id, count(*) AS lab_count
FROM alive_events
-- ***** your code below *****


---- med
SELECT event_id, count(*) AS med_count
FROM alive_events
-- ***** your code below *****




-- dead patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM dead_events
-- ***** your code below *****


---- lab
SELECT event_id, count(*) AS lab_count
FROM dead_events
-- ***** your code below *****


---- med
SELECT event_id, count(*) AS med_count
FROM dead_events
-- ***** your code below *****









