// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package db2

var (
	opQueryTemplates = map[string]string{
		// maxLsn is the max Log Sequence Number stored in the staging tables.
		// This is used during live capture of events
		"maxLsn": `
				SELECT Max(t.synchpoint)
				FROM   (
							SELECT cd_new_synchpoint AS synchpoint
							FROM   %[1]s.ibmsnap_register
							UNION ALL
							SELECT synchpoint AS synchpoint
							FROM   %[1]s.ibmsnap_register) t`,

		// nexLsn is the Log Sequence Number of the next batch from monitoring table.
		// This is used during catch up, to batch mutations.
		// The monitoring table is refreshed periodically based on the MONITOR_INTERVAL parameter as
		// set it the ASNCDC.IBMSNAP_CAPPARMS table. By default, it set to 300 seconds.
		"nextLsn": `
				SELECT Min(restart_seq)
				FROM   %[1]s.ibmsnap_capmon
				WHERE  cd_rows_inserted > 0
				AND    restart_seq > ?`,
		// get tables with mutations across all the schemata.
		"stagingTables": `
				SELECT    r.source_owner,
						r.source_table,
						r.cd_owner,
						r.cd_table
				FROM      %[1]s.ibmsnap_register r
				LEFT JOIN syscat.tables t
				ON        r.source_owner = t.tabschema
				AND       r.source_table = t.tabname
				WHERE     r.source_owner <> ''`,
		// get tables with mutations within a schema
		"stagingTablesForSchema": `
				SELECT    r.source_owner,
						r.source_table,
						r.cd_owner,
						r.cd_table
				FROM      %[1]s.ibmsnap_register r
				LEFT JOIN syscat.tables t
				ON        r.source_owner = t.tabschema
				AND       r.source_table = t.tabname
				WHERE     r.source_owner = ?
	   `,
	}

	// The IBMSNAP_UOW table provides additional information about transactions
	// that have been committed to a source table, such as a timestamp.
	// We join the IBMSNAP_UOW and change data (CD) tables based on matching
	// IBMSNAP_COMMITSEQ values when to propose changes to the target tables.
	//
	// Note: we don't consider deletes before updates
	//       (we only look at the values after the update)
	changeQuery = `
	SELECT *
		FROM   (SELECT CASE
						WHEN ibmsnap_operation = 'D'
							AND ( Lead(cdc.ibmsnap_operation, 1, 'X')
									OVER (
										PARTITION BY cdc.ibmsnap_commitseq
										ORDER BY cdc.ibmsnap_intentseq) ) = 'I' THEN 0
						WHEN ibmsnap_operation = 'I'
							AND ( Lag(cdc.ibmsnap_operation, 1, 'X')
									OVER (
										PARTITION BY cdc.ibmsnap_commitseq
										ORDER BY cdc.ibmsnap_intentseq) ) = 'D' THEN 3
						WHEN ibmsnap_operation = 'D' THEN 1
						WHEN ibmsnap_operation = 'I' THEN 2
					END OPCODE,
					uow.IBMSNAP_LOGMARKER,
					cdc.*
				FROM  %[1]s cdc ,  %[2]s.IBMSNAP_UOW uow
				WHERE  cdc.ibmsnap_commitseq > ?
					AND cdc.ibmsnap_commitseq <= ?
					AND uow.ibmsnap_commitseq=cdc.ibmsnap_commitseq 
				ORDER BY cdc.ibmsnap_commitseq,
						cdc.ibmsnap_intentseq)
		WHERE  opcode != 0 
	`
	columnQuery = `
	SELECT c.colname,
		c.keyseq,
		c.typename
	FROM   syscat.tables AS t
		INNER JOIN syscat.columns AS c
				ON t.tabname = c.tabname
				AND t.tabschema = c.tabschema
	WHERE  t.tabschema = ?
		AND t.tabname = ?
	ORDER  BY c.colno 
	`
)
