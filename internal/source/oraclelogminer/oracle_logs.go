// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package oraclelogminer

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/oraclelogminer/scn"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// StmtOperation stands for the operation types supported by replicator.
// Any changefeed with operation not one of INSERT, UPDATE or DELETE is not processed by the
// replicator.
type StmtOperation string

const (
	// OperationInsert stands for the INSERT op.
	OperationInsert StmtOperation = `INSERT`
	// OperationUpdate stands for the UPDATE op.
	OperationUpdate StmtOperation = `UPDATE`
	// OperationDelete stands for the DELETE op.
	OperationDelete StmtOperation = `DELETE`
)

// RedoLog is the changefeed structure returned by the logMiner.
type RedoLog struct {
	CommitSCN  string
	CommitTS   string
	Operation  StmtOperation
	RowID      string
	SCN        string
	SegOwner   string
	SeqNum     int
	SQLRedo    string
	SQLUndo    string
	StartSCN   string
	StartTS    string
	TableName  string
	TableSpace string
	// The timestamp stored in logMiner is of date format.
	Timestamp string
	TxnID     []byte
	UserName  string
}

// LogFileGroup is holds the meta info for log file groups from an oracle source, which are supplied
// to logMiner to get changefeed.
type LogFileGroup struct {
	GroupNumber  int
	LogFilePaths []string
	StartSCN     string
	EndSCN       string
}

// String returns a string representation of a log file group
func (g LogFileGroup) String() string {
	return fmt.Sprintf("Group#:%d, FilePaths:%s, StartSCN:%s, EndSCN:%s", g.GroupNumber, g.LogFilePaths, g.StartSCN, g.EndSCN)
}

// For more info about how to upload logs for logMiner:
// https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html
const (
	getAllLogFilesStmt = `
	SELECT
		l.GROUP#,
		lf.MEMBER,
		l.FIRST_CHANGE# AS START_SCN,
		l.NEXT_CHANGE# AS END_SCN
	FROM
		V$LOG l
	JOIN
		V$LOGFILE lf
	ON
		l.GROUP# = lf.GROUP#
	WHERE
		l.FIRST_CHANGE# <> 0 AND l.NEXT_CHANGE# <> 0
	ORDER BY
		l.GROUP#`
	getCurrentSCN            = `SELECT CURRENT_SCN FROM V$DATABASE`
	initLogMinerFileAnalyzer = `
	BEGIN
		DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => DBMS_LOGMNR.NEW);
	END;`

	addFilToLogMiner = `	
    BEGIN
		DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => DBMS_LOGMNR.ADDFILE);
	END;`

	startLogMiner = `
    BEGIN
		DBMS_LOGMNR.START_LOGMNR(
			STARTSCN => :1,  
			ENDSCN => :2, 
			OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY);
	END;`
	queryRedoLogs = `
	SELECT 
		SCN, 
		START_SCN, 
		COMMIT_SCN, 
		TIMESTAMP, 
		START_TIMESTAMP, 
		COMMIT_TIMESTAMP, 
		OPERATION, 
		SEG_OWNER, 
		TABLE_NAME, 
		TABLE_SPACE, 
		ROW_ID, 
		XID,
		SEQUENCE#,
		USERNAME, 
		SQL_REDO, 
		SQL_UNDO
	FROM V$LOGMNR_CONTENTS WHERE SEG_OWNER = '%s' ORDER BY COMMIT_SCN, XID, SEQUENCE#`
)

// readLogsOnce retrieves redo logs starting from currSCN. Since it’s
// invoked iteratively, it captures the system’s latest SCN before
// starting LogMiner, uses that SCN as the EndSCN for the current
// LogMiner session, and returns it. This returned SCN will serve as the
// starting SCN for the subsequent readLogsOnce call.
func readLogsOnce(
	ctx *stopper.Context, db *types.SourcePool, currSCN scn.SCN, sourceSchema string,
) ([]RedoLog, scn.SCN, error) {
	res := make([]RedoLog, 0)
	logFileGroups := make([]LogFileGroup, 0)

	endSCNLogMinerExec := scn.SCN{}

	rows, err := db.QueryContext(ctx, getAllLogFilesStmt)
	if err != nil {
		return nil, endSCNLogMinerExec, err
	}

	defer rows.Close()

	for rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to get changefeed from logMiner")
		}
		var logPaths, startSCN, endSCN sql.NullString
		var groupNum sql.NullInt64
		if err := rows.Scan(&groupNum, &logPaths, &startSCN, &endSCN); err != nil {
			return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to get the redo log")
		}
		if !groupNum.Valid || !logPaths.Valid || !startSCN.Valid || !endSCN.Valid {
			return nil, endSCNLogMinerExec, errors.New("failed to get valid entry of redo log")
		}
		newLogFileGroup := LogFileGroup{
			GroupNumber: int(groupNum.Int64),
			StartSCN:    startSCN.String,
			EndSCN:      endSCN.String,
		}
		// TODO(janexing): figure out the real separator here.
		newLogFileGroup.LogFilePaths = strings.Split(logPaths.String, ",")
		logFileGroups = append(logFileGroups, newLogFileGroup)
	}

	// Get the current SCN as the end scn for LogMiner execution. This SCN will serve as the start scn
	// for the next pull iteration.
	var endSCNLogMinerExecStr sql.NullString
	if err := db.QueryRowContext(ctx, getCurrentSCN).Scan(&endSCNLogMinerExecStr); err != nil {
		return nil, endSCNLogMinerExec, errors.Wrapf(err,
			"failed to get the current scn as the end of log miner execution",
		)
	}

	// TODO(janexing): we now add all files to the LogMiner but maybe we should only upload those
	// where the given SCN is in their range.
	for i, logFileGroup := range logFileGroups {
		for j, logPath := range logFileGroup.LogFilePaths {
			if i == 0 && j == 0 {
				if _, err := db.ExecContext(ctx, initLogMinerFileAnalyzer, logPath); err != nil {
					return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to init the LogMiner file upload")
				}
			} else {
				if _, err := db.ExecContext(ctx, addFilToLogMiner, logPath); err != nil {
					return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to add file %s to the LogMiner", logPath)
				}
			}
		}
		log.Debugf("loaded %s", logFileGroup)
	}

	// Have the logminer start analyze the uploaded logs.
	if _, err := db.ExecContext(ctx, startLogMiner, currSCN.Val, endSCNLogMinerExecStr.String); err != nil {
		return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to start the logminer for starting scn %s", currSCN)
	}

	q := fmt.Sprintf(queryRedoLogs, sourceSchema)
	// q := fmt.Sprintf(queryRedoLogs, db.config.SourceSchema.Raw())
	redoLogRows, err := db.QueryContext(ctx, q)
	log.Debugf("getting redo log rows: %s", q)
	if err != nil {
		return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to read from V$LOGMNR_CONTENTS for log content with start scn %s", currSCN)
	}

	defer redoLogRows.Close()

	for redoLogRows.Next() {
		redoLog := RedoLog{}

		var xid []byte
		var scn, startSCN, commitSCN, commitTS, ts, startTS, op, segOwner, tblName, tblSpace, rowID, username, sqlRedo, sqlUndo sql.NullString
		var seqNum sql.NullInt64

		if err := redoLogRows.Scan(
			&scn,
			&startSCN,
			&commitSCN,
			&ts,
			&startTS,
			&commitTS,
			&op,
			&segOwner,
			&tblName,
			&tblSpace,
			&rowID,
			&xid,
			&seqNum,
			&username,
			&sqlRedo,
			&sqlUndo,
		); err != nil {
			return nil, endSCNLogMinerExec, errors.Wrapf(err, "failed to scan for the redo log")
		}

		if scn.Valid {
			redoLog.SCN = scn.String
		}

		if startSCN.Valid {
			redoLog.StartSCN = startSCN.String
		}

		if commitSCN.Valid {
			redoLog.CommitSCN = commitSCN.String
		}

		if op.Valid {
			redoLog.Operation = StmtOperation(op.String)
		}

		if segOwner.Valid {
			redoLog.SegOwner = segOwner.String
		}

		if tblName.Valid {
			redoLog.TableName = tblName.String
		}

		if tblSpace.Valid {
			redoLog.TableSpace = tblSpace.String
		}

		if rowID.Valid {
			redoLog.RowID = rowID.String
		}

		if username.Valid {
			redoLog.UserName = username.String
		}

		if sqlRedo.Valid {
			redoLog.SQLRedo = sqlRedo.String
		}

		if sqlUndo.Valid {
			redoLog.SQLUndo = sqlUndo.String
		}

		redoLog.TxnID = xid

		if seqNum.Valid {
			redoLog.SeqNum = int(seqNum.Int64)
		}

		if redoLog.Operation != OperationInsert &&
			redoLog.Operation != OperationUpdate &&
			redoLog.Operation != OperationDelete {
			log.Warnf("log ignored as it is not DML: %s", sqlRedo.String)
			continue
		}

		res = append(res, redoLog)
	}

	log.Infof("redo logs read until SCN %s", endSCNLogMinerExecStr.String)
	endSCNLogMinerExec, err = scn.ParseStringToSCN(endSCNLogMinerExecStr.String)
	if err != nil {
		return nil, scn.SCN{}, err
	}
	return res, endSCNLogMinerExec, nil
}
