package oracleparser

import (
	"fmt"
	"go/constant"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const (
	singleQuote = '\''
	doubleQuote = '"'
)

type lexer struct {
	in  string
	pos int

	lastToken sqlSymType
	lastError error
}

func newLexer(str string) *lexer {
	return &lexer{in: str}
}

func (l *lexer) Lex(lval *sqlSymType) int {
	if l.done() {
		lval.SetID(0)
		lval.SetPos(int32(len(l.in)))
		lval.SetStr("EOF")
		return 0
	}
	l.lex(lval)
	l.lastToken = *lval
	return int(lval.id)
}

func (l *lexer) done() bool {
	return l.pos >= len(l.in)
}

func (l *lexer) scanUntilEndQuote(
	lval *sqlSymType, quoteCh rune, normFunc func(string) string, id int32,
) {
	str := ""
	for l.pos < len(l.in) {
		nextCh := l.next()
		// Double quotes = 1 single quote.
		if nextCh == quoteCh {
			if l.peek() == quoteCh {
				l.next()
			} else {
				lval.SetID(id)
				if normFunc != nil {
					str = normFunc(str)
				}
				lval.SetStr(str)
				return
			}
		}
		str += string(nextCh)
	}
	lval.SetStr(fmt.Sprintf("unfinished quote: %c", quoteCh))
}

func (l *lexer) peek() rune {
	ch, _ := utf8.DecodeRuneInString(l.in[l.pos:])
	return ch
}

func (l *lexer) next() rune {
	ch, sz := utf8.DecodeRuneInString(l.in[l.pos:])
	l.pos += sz
	return ch
}

func (l *lexer) skipSpace() {
	if inc := strings.IndexFunc(l.in[l.pos:], func(r rune) bool { return !unicode.IsSpace(r) }); inc > 0 {
		l.pos += inc
	}
}

func (l *lexer) lex(lval *sqlSymType) {
	l.skipSpace()

	lval.SetUnionVal(nil)
	startPos := l.pos
	lval.SetPos(int32(startPos))
	ch := l.next()
	switch {
	case ch == singleQuote:
		l.scanUntilEndQuote(lval, singleQuote, nil, SCONST)
	case ch == doubleQuote:
		l.scanUntilEndQuote(lval, doubleQuote, NormalizeString, IDENT)
	case unicode.IsDigit(ch):
		curPos := l.pos
	scanNumLoop:
		for curPos < len(l.in) {
			nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
			switch {
			case unicode.IsDigit(nextCh):
			default:
				break scanNumLoop
			}
			curPos += sz
		}
		lval.SetStr(l.in[startPos:curPos])
		l.pos = curPos
		val, err := strconv.ParseUint(lval.Str(), 10, 64)
		if err != nil {
			l.lexerError(lval, err)
			return
		}
		lval.SetUnionVal(tree.NewNumVal(constant.MakeUint64(val), lval.Str(), false))
	case isIdentifier(ch, true):
		curPos := l.pos
	scanChLoop:
		for curPos < len(l.in) {
			nextCh, sz := utf8.DecodeRuneInString(l.in[curPos:])
			switch {
			case isIdentifier(nextCh, false):
				break scanChLoop
			default:
				break scanChLoop
			}
			curPos += sz
		}
		str := l.in[startPos:curPos]
		lval.SetStr(str)
		l.pos = curPos
	default:
		// Otherwise, it is the character itself.
		lval.SetID(ch)
		lval.SetStr(string(ch))
	}
}

const (
	identifierUnicodeMin = 200
	identifierUnicodeMax = 377
)

func isIdentifier(ch rune, firstCh bool) bool {
	if (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= identifierUnicodeMin && ch <= identifierUnicodeMax) || ch == '_' {
		return true
	}
	return !firstCh && unicode.IsDigit(ch)
}

func isHexDigit(nextCh rune) bool {
	return (nextCh >= 'A' && nextCh <= 'F') || (nextCh >= 'a' && nextCh <= 'f') || (nextCh >= '0' && nextCh <= '9')
}

func (l *lexer) lexerError(lval *sqlSymType, err error) {
	l.lastToken = *lval
	lval.id = ERROR
	l.Error(err.Error())
}

func (l *lexer) Error(e string) {
	e = strings.TrimPrefix(e, "syntax error: ") // we'll add it again below.
	err := pgerror.WithCandidateCode(errors.Newf("%s", e), pgcode.Syntax)
	lastTok := l.lastToken
	l.lastError = parser.PopulateErrorDetails(lastTok.id, lastTok.str, lastTok.pos, err, l.in)
}
