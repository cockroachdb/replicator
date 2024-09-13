SELECT log_mode FROM v$database;
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
SELECT log_mode FROM v$database;


ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
select supplemental_log_data_min, supplemental_log_data_pk from v$database;