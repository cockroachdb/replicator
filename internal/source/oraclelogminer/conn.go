package oraclelogminer

import "database/sql"

type DB struct {
	ConnStr string
	*sql.DB
	UserName string
	SCN      string
}

//type Conn struct {
//	*sql.Conn
//	SCN string
//}
//
//func (db *DB) NewConn(ctx *stopper.Context) (*Conn, error) {
//	res, err := db.Conn(ctx)
//	if err != nil {
//		return nil, errors.Wrapf(err, "failed to create new connection for oracle logminer for user %s", db.UserName)
//	}
//	return &Conn{Conn: res}, nil
//}
