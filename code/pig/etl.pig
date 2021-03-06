-- ***************************************************************************
-- TASK
-- Aggregate events into features of patient and generate training, testing data for mortality prediction.
-- Steps have been provided to guide you.
-- You can include as many intermediate steps as required to complete the calculations.
-- ***************************************************************************

-- ***************************************************************************
-- TESTS
-- To test, please change the LOAD path for events and mortality to ../../test/events.csv and ../../test/mortality.csv
-- 6 tests have been provided to test all the subparts in this exercise.
-- Manually compare the output of each test against the csv's in test/expected folder.
-- ***************************************************************************

-- register a python UDF for converting data into SVMLight format
REGISTER utils.py USING jython AS utils;

-- load events file
-- events = LOAD '../../data/events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);
events = LOAD '../sample_test/sample_events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);

-- select required columns from events
events = FOREACH events GENERATE patientid, eventid, ToDate(timestamp, 'yyyy-MM-dd') AS etimestamp, value;

-- load mortality file
-- mortality = LOAD '../../data/mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);
mortality = LOAD '../sample_test/sample_mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);

mortality = FOREACH mortality GENERATE patientid, ToDate(timestamp, 'yyyy-MM-dd') AS mtimestamp, label;

--To display the relation, use the dump command e.g. DUMP mortality;

-- ***************************************************************************
-- Compute the index dates for dead and alive patients
-- ***************************************************************************
eventswithmort = JOIN events BY patientid, mortality BY patientid;
-- -- eventsdead = FOREACH eventswithmort GENERATE events::patientid, eventid, value, label, etimestamp, AddDuration(mtimestamp, 'P-30D') as indexdate;
-- deadevents = FOREACH eventsdead GENERATE patientid, eventid, value, label, DaysBetween(etimestamp, indexdate) as time_difference;
deadevents = FOREACH eventswithmort GENERATE mortality::patientid, eventid, value, label, DaysBetween(SubtractDuration(mtimestamp, 'P30D'), etimestamp) as time_difference;


-- deadevents = -- detect the events of dead patients and create it of the form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp
alive = JOIN events BY patientid LEFT OUTER, mortality by patientid;
alive = FILTER alive BY mtimestamp is null;
alive = FOREACH alive GENERATE events::patientid as patientid, events::eventid, events::value, 0 as label, etimestamp;
alive_grouped = GROUP alive BY patientid;
alive_by_pid = FOREACH alive_grouped GENERATE group as pid, MAX(alive.etimestamp) as maxtimestamp;
alive = JOIN alive BY patientid, alive_by_pid BY pid;
aliveevents = FOREACH alive GENERATE patientid, events::eventid, events::value, label, DaysBetween(maxtimestamp, etimestamp) as time_difference;

-- aliveevents = -- detect the events of alive patients and create it of the form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp

--TEST-1
deadevents = ORDER deadevents BY patientid, eventid;
aliveevents = ORDER aliveevents BY patientid, eventid;
STORE aliveevents INTO 'aliveevents' USING PigStorage(',');
STORE deadevents INTO 'deadevents' USING PigStorage(',');

-- ***************************************************************************
-- Filter events within the observation window and remove events with missing values
-- ***************************************************************************
-- combined = UNION aliveevents, deadevents; 
-- filtered = FILTER combined BY value IS NOT null;
-- filtered = FILTER filtered BY time_difference <= 2000; -- contains only events for all patients within the observation window of 2000 days and is of the form (patientid, eventid, value, label, time_difference)
-- filtered = FILTER combined BY time_difference >= 2000; -- contains only events for all patients within the observation window of 2000 days and is of the form (patientid, eventid, value, label, time_difference)

filtered = UNION aliveevents, deadevents;
filtered = FILTER filtered BY (value IS NOT null) AND (time_difference <= 2000) AND (time_difference >= 0);

--TEST-2
filteredgrpd = GROUP filtered BY 1;
filtered = FOREACH filteredgrpd GENERATE FLATTEN(filtered);
filtered = ORDER filtered BY patientid, eventid,time_difference;
STORE filtered INTO 'filtered' USING PigStorage(',');

-- ***************************************************************************
-- Aggregate events to create features
-- ***************************************************************************
featureswithid = GROUP filtered BY (patientid, eventid);
featureswithid = FOREACH featureswithid GENERATE FLATTEN(group), COUNT(filtered.value) as featurevalue;

-- for group of (patientid, eventid), count the number of  events occurred for the patient and create relation of the form (patientid, eventid, featurevalue)

--TEST-3
featureswithid = ORDER featureswithid BY patientid, eventid;
STORE featureswithid INTO 'features_aggregate' USING PigStorage(',');

-- ***************************************************************************
-- Generate feature mapping
-- ***************************************************************************
-- all_features = GROUP featureswithid BY eventid;
-- all_features = FOREACH all_features GENERATE group.eventid as eventid, COUNT(patientid) as count;
-- all_features = RANK all_features BY count ASC;
-- all_features = FOREACH all_features GENERATE rank_all_features - 1 AS idx, eventid;
all_features = DISTINCT(FOREACH featureswithid GENERATE eventid);
all_features = ORDER all_features BY eventid;
all_features = RANK all_features BY eventid;
all_features = FOREACH all_features GENERATE ($0 - 1) AS idx, $1;
 -- compute the set of distinct eventids obtained from previous step, sort them by eventid and then rank these features by eventid to create (idx, eventid). Rank should start from 0.

-- store the features as an output file
STORE all_features INTO 'features' using PigStorage(' ');

features = JOIN featureswithid BY eventid, all_features BY eventid;

--TEST-4
features = ORDER features BY patientid, idx;
features = FOREACH features GENERATE patientid, idx, featurevalue;
features = ORDER features BY patientid, idx;

STORE features INTO 'features_map' USING PigStorage(',');

-- ***************************************************************************
-- Normalize the values using min-max normalization
-- Use DOUBLE precision
-- ***************************************************************************
maxvalues = GROUP features BY idx;
maxvalues = FOREACH maxvalues GENERATE group as idx, MAX(features.featurevalue) AS maxvalues;

normalized = JOIN features BY idx, maxvalues BY idx;

features = FOREACH normalized GENERATE patientid, features::all_features::idx as idx, ((double)featurevalue/(double)maxvalues) as normalizedfeaturevalue;

--TEST-5
features = ORDER features BY patientid, idx;
STORE features INTO 'features_normalized' USING PigStorage(',');

-- ***************************************************************************
-- Generate features in svmlight format
-- features is of the form (patientid, idx, normalizedfeaturevalue) and is the output of the previous step
-- e.g.  1,1,1.0
--  	 1,3,0.8
--	     2,1,0.5
--       3,3,1.0
-- ***************************************************************************

grpd = GROUP features BY patientid;
grpd_order = ORDER grpd BY $0;
features = FOREACH grpd_order
{
    sorted = ORDER features BY idx;
    generate group as patientid, utils.bag_to_svmlight(sorted) as sparsefeature;
}

-- ***************************************************************************
-- Split into train and test set
-- labels is of the form (patientid, label) and contains all patientids followed by label of 1 for dead and 0 for alive
-- e.g. 1,1
--	2,0
--      3,1
-- ***************************************************************************
labels = GROUP filtered by patientid;
labels = FOREACH labels GENERATE group as patientid, MIN(filtered.label);

--Generate sparsefeature vector relation
samples = JOIN features BY patientid, labels BY patientid;
samples = DISTINCT samples PARALLEL 1;
samples = ORDER samples BY $0;
samples = FOREACH samples GENERATE $3 AS label, $1 AS sparsefeature;

--TEST-6
STORE samples INTO 'samples' USING PigStorage(' ');

-- randomly split data for training and testing
DEFINE rand_gen RANDOM('6505');
samples = FOREACH samples GENERATE rand_gen() as assignmentkey, *;
SPLIT samples INTO testing IF assignmentkey <= 0.20, training OTHERWISE;
training = FOREACH training GENERATE $1..;
testing = FOREACH testing GENERATE $1..;

-- save training and tesing data
STORE testing INTO 'testing' USING PigStorage(' ');
STORE training INTO 'training' USING PigStorage(' ');
