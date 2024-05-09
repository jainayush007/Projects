CREATE OR REPLACE PROCEDURE `ayush-agrolis.curated.sp_dq_results_details` (in_table_name STRING)
BEGIN

DECLARE rowcount INT64 DEFAULT 1;
DECLARE v_rownum INT64 DEFAULT 1;
DECLARE QUERIES STRING;
DECLARE v_dq_job_id STRING;
--DECLARE in_table_name STRING DEFAULT 'ayush-agrolis.curated.taxi_trips_ps';

BEGIN
create temp table rule_failed_records_query_tbl
as(
  SELECT dq_job_id, row_number() OVER() rownum, REGEXP_REPLACE(rule_failed_records_query, "[)]" || " SELECT " || "[*]",fields) rule_failed_records_query 
  from
      (
            (SELECT dq_job_id,') SELECT "' || dq_job_id || '",TIMESTAMP("'||job_start_time || '") AS job_start_time,"' || table_name || '","' || rule_column ||'",CAST(' || rule_column ||' AS STRING) ,"'|| incremental ||'",'||  'message_id, publish_time, CURRENT_TIMESTAMP() '  fields , REGEXP_REPLACE(rule_failed_records_query,'WITH ','INSERT INTO ayush-agrolis.curated.dq_failures_stg WITH ') rule_failed_records_query
                  FROM
                        (SELECT  data_quality_job_id dq_job_id, data_source.table_project_id||'.'||data_source.dataset_id||'.'||data_source.table_id table_name,rule_column, job_start_time, JSON_VALUE(data_quality_job_configuration.incremental) AS incremental, rule_failed_records_query
                        FROM `ayush-agrolis.test_us.results_table` 
                        WHERE rule_evaluation_type='Per row') a
              WHERE a.table_name=in_table_name
            qualify rank() over(partition by table_name order by job_start_time desc)=1
            )
      )
);

SET rowcount= (select max(rownum) from rule_failed_records_query_tbl);
SET v_dq_job_id = (select distinct dq_job_id FROM rule_failed_records_query_tbl);

DELETE FROM `ayush-agrolis.curated.dq_failures_stg` where dq_job_id=v_dq_job_id;

WHILE (v_rownum <= rowcount) DO
  SET QUERIES = (SELECT rule_failed_records_query FROM rule_failed_records_query_tbl where rownum = v_rownum);

  SELECT QUERIES;

  EXECUTE IMMEDIATE QUERIES;

  SET v_rownum= v_rownum+1;
END WHILE;

END;


IF "true" = (select distinct incremental FROM `ayush-agrolis.curated.dq_failures_stg` where dq_job_id=v_dq_job_id)
THEN 
    DELETE FROM `ayush-agrolis.curated.dq_failures` WHERE id in (SELECT dq.id FROM `ayush-agrolis.curated.dq_failures` dq JOIN  `ayush-agrolis.curated.taxi_trips_ps` trips on dq.id=trips.message_id where trips.publish_time > dq.table_incremental_ts and trips.publish_time < (select max(job_start_time) from `ayush-agrolis.curated.dq_failures_stg` where dq_job_id=v_dq_job_id ));
ELSE
    DELETE FROM `ayush-agrolis.curated.dq_failures` WHERE table_name=in_table_name;
END IF;

INSERT INTO `ayush-agrolis.curated.dq_failures`
select table_name,id,table_incremental_ts,current_timestamp() from `ayush-agrolis.curated.dq_failures_stg` where dq_job_id=v_dq_job_id;

END;
