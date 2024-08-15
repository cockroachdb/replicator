# Oracle Parser

The Oracle parser uses the [ANTLR4](https://github.com/antlr/antlr4) library to
parse an ANTLR4 grammar file. PL/SQL (the language behind oracle) is parsed
using the `.g4` files [here](plsqldefs).

To add parsing capabilities, we simply expand the `Listener` interface to accept
more input.

## Re-generating the language
If there are new language features in the `.g4` that we want to include, we will
need to regenerate the files.

* Install the [ANTLR4 complete jar and a java environment](install) that can run it.
* Download `PlSqlLexer.g4` and `PlSqlParser.g4` from [the definitions](plsqldefs).
* Run `java -jar /path/to/antlr4.jar -Dlanguage=Go -no-visitor -package parser *.g4` (replace `/path/to/antlr4.jar`).
* Replace all uses of `self` in `plsql_lexer.go` and `plsql_parser.go` with `p.`.
  See [here](golangissue) for details.
* Re-run gofmt: `gofmt -w *.go`.

tl;dr: `antlr -Dlanguage=Go -no-visitor -package parser *.g4 && gofmt -w *.go && sed -i '' -e 's/self\./p\./g' plsql_*.go`

[plsqldefs]: https://github.com/antlr/grammars-v4/tree/master/sql/plsql
[install]: https://github.com/antlr/antlr4/blob/master/doc/getting-started.md#installation
[golangissue]: https://github.com/antlr/grammars-v4/issues/1588