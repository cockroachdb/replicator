%{
    package oracleparser
%}

%{

func (s *sqlSymType) ID() int32 {
  return s.id
}

func (s *sqlSymType) SetID(id int32) {
  s.id = id
}

func (s *sqlSymType) Pos() int32 {
  return s.pos
}

func (s *sqlSymType) SetPos(pos int32) {
  s.pos = pos
}

func (s *sqlSymType) Str() string {
  return s.str
}

func (s *sqlSymType) SetStr(str string) {
  s.str = str
}

func (s *sqlSymType) UnionVal() interface{} {
  return s.union.val
}

func (s *sqlSymType) SetUnionVal(val interface{}) {
  s.union.val = val
}


type sqlSymUnion struct {
  val interface{}
}
%}


%union {
  id    int32
  pos   int32
  str   string
  union sqlSymUnion
}

%token SELECT FROM WHERE IDENTIFIER STRING_LITERAL
%token <str> UPDATE TABLE SET
%token <str> SCONST IDENT
%token <str> WHEN WHERE
%token ERROR

// Precedence: lowest to highest
%nonassoc  VALUES              // see value_clause
%nonassoc  SET                 // see table_expr_opt_alias_idx
%left      UNION EXCEPT
%left      INTERSECT
%left      OR
%left      AND
%right     NOT
%nonassoc  IS ISNULL NOTNULL   // IS sets precedence for IS NULL, etc
%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc  '~' BETWEEN IN LIKE ILIKE SIMILAR NOT_REGMATCH REGIMATCH NOT_REGIMATCH NOT_LA
%nonassoc  ESCAPE              // ESCAPE must be just above LIKE/ILIKE/SIMILAR
%nonassoc  CONTAINS CONTAINED_BY '?' JSON_SOME_EXISTS JSON_ALL_EXISTS
%nonassoc  OVERLAPS
%left      POSTFIXOP           // dummy for postfix OP rules

%%
statement:
    select_stmt
    ;

select_stmt:
    SELECT IDENTIFIER FROM IDENTIFIER opt_where_clause
    ;

opt_where_clause:
    /* empty */
    |
    WHERE IDENTIFIER '=' STRING_LITERAL
    ;

%%

