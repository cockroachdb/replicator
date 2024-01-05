-- Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
-- https://github.com/debezium/debezium-examples/tree/main/tutorial/debezium-db2-init/db2server

VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');

CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS' ); 
CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS_ON_HAND' ); 
CALL ASNCDC.ADDTABLE('DB2INST1', 'CUSTOMERS' );
CALL ASNCDC.ADDTABLE('DB2INST1', 'ORDERS' ); 

VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');