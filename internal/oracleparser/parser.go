package oracleparser

import "github.com/cockroachdb/errors"

func Parse(sql string) (string, error) {
	lexer := newLexer(sql)
	p := sqlNewParser()
	if p.Parse(lexer) != 0 {
		if lexer.lastError == nil {
			return "", errors.AssertionFailedf("expected lexer error but got none")
		}
		return "", lexer.lastError
	}

	return sql, nil
}
