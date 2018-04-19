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

[0-9]+(\.[0-9]*)?([eE][+-]?[0-9]+)? return 'NUMBER_LITERAL';
\".*\"         return 'STRING_LITERAL';
[TRUE|FALSE]   return 'BOOLEAN_LITERAL';

[0-9a-zA-Z]+   return 'USER_IDENTIFIER';
:[0-9a-zA-Z]+  return 'SYSTEM_IDENTIFIER';

$      return 'EOF';

/lex

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
    ;

select
    : "SELECT" select-list where-clause group-by-clause order-by-clause limit-clause offset-clause
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
    | selection "," expression-select-list
    ;

system-star
    : ":*"
    ;

user-star
    : "*"
    ;

selection
    : expression
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
    | "STRING_LITERAL"
    | "BOOLEAN_LITERAL"
    | "NULL"
    ;
%%