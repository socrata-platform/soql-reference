%lex

%%
\s+      /* skip whitespace */

"AND"     return 'AND';
"AS"      return 'AS';
"ASC"     return 'ASC';
"BETWEEN" return 'BETWEEN';
"BY"      return 'BY';
"DESC"    return 'DESC';
"DISTINCT" return 'DISTINCT';
"EXCEPT"  return 'EXCEPT';
"FROM"    return 'FROM';
"FULL"    return 'FULL';
"FIRST"   return 'FIRST';
"GROUP"   return 'GROUP';
"IN"      return 'IN';
"INNER"   return 'INNER';
"IS"      return 'IS';
"JOIN"    return 'JOIN';
"LAST"    return 'LAST';
"LEFT"    return 'LEFT';
"LIMIT"   return 'LIMIT';
"NOT"     return 'NOT';
"NULL"    return 'NULL';
"OFFSET"  return 'OFFSET';
"ON"      return 'ON';
"OR"      return 'OR';
"ORDER"   return 'ORDER';
"OUTER"   return 'OUTER';
"RIGHT"   return 'RIGHT';
"SELECT"  return 'SELECT';
"WHERE"   return 'WHERE';

","     return ',';
"*"     return '*';
":*"    return ':*';
"("     return "(";
")"     return ")";

"=="    return "==";
"="     return "=";
"!="    return "!=";
"<>"    return "<>";
"<"     return "<";
"<="    return "<=";
">="    return ">=";
">"     return ">";

"+"     return "+";
"-"     return "-";
"||"    return "||";
"/"     return "/";
"%"     return "%";
"^"     return "^";

"::"    return "::";
"."     return ".";
"["     return "[";
"]"     return "]";

[0-9]+    return 'INTEGER_LITERAL';
[0-9]+(\.[0-9]*)?([eE][+-]?[0-9]+)? return 'NUMBER_LITERAL';
\".*\"         return 'STRING_LITERAL';
[TRUE|FALSE]   return 'BOOLEAN_LITERAL';

[0-9a-zA-Z]+   return 'USER_IDENTIFIER';
:[0-9a-zA-Z]+  return 'SYSTEM_IDENTIFIER';

$      return 'EOF';

/lex

/* operator associations and precedence */

/* "(" ")"
 * "." "[" "]"
 * "::"
 * "+" "-" (unary)
 */

%right "^"
%left "*" "/" "%"
%left "+" "-" "||"
%left "=" "==" "!=" "<>" "<" "<=" ">=" ">"
%left "IS" "BETWEEN" "IN"
%left "NOT"
%left "AND"
%left "OR"

%start query
%%

/* Select */
query
    : select EOF
        {return $1;}
    ;

select
    : "SELECT" select-list where-clause group-by-clause order-by-clause limit-clause offset-clause
        {$$ = {
          'select-list': $2,
          'where-clause': $3,
          'group-by-clause': $4,
          'order-by-clause': $5,
          'limit-clause': $6,
          'offset-clause': $7
          };
        }
    ;

/* Selection lists */
select-list
    : system-star 
    | system-star "," only-user-star-select-list
    | only-user-star-select-list
    ;

only-user-star-select-list
    : user-star
    | user-star "," expression-select-list
    | expression-select-list
    ;

expression-select-list
    : selection
      {
        $$ = {
          type: 'selection',
          arguments: [$1]
        };
      }
    | selection "," expression-select-list
      {
        $$ = {
          type: 'selection-list',
          arguments: [$1]
        };
      }
    ;

system-star
    : ":*"
    ;

user-star
    : "*"
    ;

selection
    : expression
      {
        $$ = {
          type: 'expression',
          arguments: [$1]
        };
      }
    | expression "AS" "USER_IDENTIFER"
    ;

/* WHERE clause */
where-clause
    : "WHERE" expression
    |
    ;

/* GROUP BY clause */
group-by-clause
    : "GROUP" "BY" expression-list having-clause
    |
    ;

/* HAVING clause */
having-clause
    : "HAVING" expression
    |
    ;

/* ORDER BY clause */
order-by-clause
    : "ORDER" "BY" ordering-list
    |
    ;

ordering-list
    : ordering
    | ordering "," ordering-list
    ;

ordering
    : expression sort-ordering null-ordering
    ;

sort-ordering
    : "ASC"
    | "DESC"
    |
    ;

null-ordering
    : "NULL" "FIRST"
    | "NULL" "LAST"
    |
    ;

/* LIMIT clause */
limit-clause
    : "LIMIT" "INTEGER_LITERAL"
    |
    ;

/* OFFSET clause */
offset-clause
    : "OFFSET" "INTEGER_LITERAL"
    |
    ;

/* Expressions */
expression
    : disjunction
    ;

disjunction
    : conjunction
    | disjunction "OR" conjunction
    ;

conjunction
    : negation
    | conjunction "AND" negation
    ;

negation
    : "NOT" negation
    | is-between-in
    ;

is-between-in
    : is-between-in "IS" "NULL"
    | is-between-in "IS" "NOT" "NULL"
    | is-between-in "BETWEEN" is-between-in "AND" is-between-in
    | is-between-in "NOT" "BETWEEN" is-between-in "AND" is-between-in
    | is-between-in "IN" "(" expression-list ")"
    | is-between-in "NOT" "IN" "(" expression-list ")"
    | order
    ;

expression-list
    : expression
    | expression "," expression-list
    ;

order
    : term
    | order "=" term
    | order "==" term
    | order "!=" term
    | order "<>" term
    | order "<" term
    | order "<=" term
    | order ">=" term
    | order ">" term
    ;

term
    : factor
    | term "+" factor
    | term "-" factor
    | term "||" factor
    ;

factor
    : exp
    | factor "*" exp
    | factor "/" exp
    | "%" exp
    ;

exp
    : unary
    | unary "^" exp
    ;

unary
    : "+" unary
    | "-" unary
    | cast
    ;

cast
    : cast "::" identifier
    | dereference
    ;

identifier
    : "USER_IDENTIFIER"
    | "SYSTEM_IDENTIFIER"
    ;

dereference
    : dereference "." identifier
    | dereference "[" expression "]"
    | value
    ;

value
    : identifier-or-funcall
    | literal
    | "(" expression ")"
    ;

identifier-or-funcall
    : identifier
    ;

literal
    : "NUMBER_LITERAL"
    | "INTEGER_LITERAL"
    | "STRING_LITERAL"
    | "BOOLEAN_LITERAL"
    | "NULL"
    ;
%%