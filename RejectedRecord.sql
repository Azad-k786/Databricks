-- Databricks notebook source
create database RejectedRecord comment "In this database store rejected record "  location '/Databases/RejectedRecord';

-- COMMAND ----------

use RejectedRecord

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("/Rejected_Records/AttendanceFile_ProjectName_Null")
-- MAGIC df.write.format("delta").mode("overwrite").option("path","/Databases/Rejectedrecord/AttendanceFile_NullRecords").saveAsTable("AttendanceFile")

-- COMMAND ----------

select * from RejectedRecord.AttendanceFile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df1=spark.read.parquet("/Rejected_Records/Employee_File")
-- MAGIC df1.write.format("delta").mode("overwrite").option("path","/Databases/Rejectedrecord/Employeefile").saveAsTable("Employeefile")

-- COMMAND ----------

select * from Rejectedrecord.Employeefile


-- COMMAND ----------

SELECT  ( 
        SELECT count(*) from AttendanceFile
        )
       +
        (
        SELECT count(*) from Employeefile
        ) 
        AS RejectedRecord_Count

-- COMMAND ----------

describe database extended RejectedRecord;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/Databases/RejectedRecord",True)

-- COMMAND ----------


