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
            'offset-clause:': $7
            };
        }
    ;

/* Selection lists */
select-list
    : system-star
        {$$ = {
            type: 'select-list',
            arguments: [$1, null]
            };
        }
    | system-star "," only-user-star-select-list
        {$$ = {
            type: 'select-list',
            arguments: [$1, $3]
            };
        }
    | only-user-star-select-list
        {$$ = {
            type: 'select-list',
            arguments: [null, $1]
            };
        }
    ;

only-user-star-select-list
    : user-star
        {$$ = {
            type: 'only-user-star-select-list',
            arguments: [$1, null]
            };
        }
    | user-star "," expression-select-list
        {$$ = {
            type: 'only-user-star-select-list',
            arguments: [$1, $3]
            };
        }
    | expression-select-list
        {$$ = {
            type: 'only-user-star-select-list',
            arguments: [null, $1]
            };
        }
    ;

expression-select-list
    : selection
        {$$ = {
            type: 'expression-select-list',
            arguments: [$1]
            };
        }
    | selection "," expression-select-list
        {$$ = {
            type: 'expression-select-list',
            arguments: [$1, $3]
            };
        }
    ;

system-star
    : ":*"
        {$$ = {
            type: 'system-star',
            arguments: []
            };
        }
    ;

user-star
    : "*"
        {$$ = {
            type: 'user-star',
            arguments: []
            };
        }
    ;

selection
    : expression
        {$$ = {
            type: 'selection',
            arguments: [$1, null]
            };
        }
    | expression "AS" user-identifier
        {$$ = {
            type: 'selection',
            arguments: [$1, $3]
            };
        }
    ;

/* WHERE clause */
where-clause
    : "WHERE" expression
        {$$ = [$2];}
    |
        {$$ = [];}
    ;

/* GROUP BY clause */
group-by-clause
    : "GROUP" "BY" expression-list having-clause
        {$$ = [$3, $4];}
    |
        {$$ = [];}
    ;

/* HAVING clause */
having-clause
    : "HAVING" expression
        {$$ = $2;}
    |
        {$$ = [];}
    ;

/* ORDER BY clause */
order-by-clause
    : "ORDER" "BY" ordering-list
        {$$ = [$3];}
    |
        {$$ = [];}
    ;

ordering-list
    : ordering
        {$$ = $1;}
    | ordering "," ordering-list
        {$$ = {
            type: 'ordering-list',
            arguments: [$1, $3]
            };
        }
    ;

ordering
    : expression sort-ordering null-ordering
        {$$ = {
            type: 'ordering',
            arguments: [$1, $2, $3]
            };
        }
    ;

sort-ordering
    : "ASC"
        {$$ = {
            type: 'sort-ordering',
            arguments: [$1]
            };
        }
    | "DESC"
        {$$ = {
            type: 'sort-ordering',
            arguments: [$1]
            };
        }
    |
    ;

null-ordering
    : "NULL" "FIRST"
        {$$ = {
            type: 'null-ordering',
            arguments: [$2]
            };
        }
    | "NULL" "LAST"
        {$$ = {
            type: 'null-ordering',
            arguments: [$2]
            };
        }
    |
    ;

/* LIMIT clause */
limit-clause
    : "LIMIT" integer-literal
        {$$ = [$2];}
    |
        {$$ = [];}
    ;

/* OFFSET clause */
offset-clause
    : "OFFSET" integer-literal
        {$$ = [$2];}
    |
        {$$ = [];}
    ;

/* Expressions */
expression
    : disjunction
        {$$ = {
            type: 'expression',
            arguments: [$1]
            };
        }
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
        {$$ = {
            type: 'is',
            arguments: [$1, {type: 'null'}]
            };
        }
    | is-between-in "IS" "NOT" "NULL"
        {$$ = {
            type: 'is-not',
            arguments: [$1, {type: 'null'}]
            };
        }
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
        {$$ = $1;}
    ;

cast
    : cast "::" identifier
        {$$ = {
            type: 'cast',
            arguments: [$1, $3]
            };
        }
    | dereference
        {$$ = $1;}
    ;

identifier
    : user-identifier
        {$$ = $1;}
    | "SYSTEM_IDENTIFIER"
        {$$ = {
            type: 'system-identifier',
            arguments: [$1]
            };
        }
    ;

user-identifier
    : "USER_IDENTIFIER"
        {$$ = {
            type: 'user-identifier',
            arguments: [$1]
            };
        }
    ;

dereference
    : dereference "." identifier
        {$$ = {
            type: 'dereference-dot',
            arguments: [$1, $3]
            };
        }
    | dereference "[" expression "]"
        {$$ = {
            type: 'dereference-brackets',
            arguments: [$1, $3]
            };
        }
    | value
        {$$ = $1;}
    ;

value
    : identifier-or-funcall
        {$$ = $1;}
    | literal
        {$$ = $1;}
    | "(" expression ")"
        {$$ = {
            type: 'expression-in-parentheses',
            arguments: [$2]
            };
        }
    ;

identifier-or-funcall
    : identifier
        {$$ = $1;}
    | identifier "(" user-star ")"
        {$$ = {
            type: 'funcall',
            arguments: [$1, $3]
            };
        }
    | identifier "(" expression-list ")"
        {$$ = {
            type: 'funcall',
            arguments: [$1, $3]
            };
        }
    ;

literal
    : "NUMBER_LITERAL"
        {$$ = {
            type: 'number-literal',
            arguments: [$1]
            };
        }
    | "INTEGER_LITERAL"
        {$$ = {
            type: 'number-literal',
            arguments: [$1]
            };
        }
    | "STRING_LITERAL"
        {$$ = {
            type: 'string-literal',
            arguments: [$1]
            };
        }
    | "BOOLEAN_LITERAL"
        {$$ = {
            type: 'boolean-literal',
            arguments: [$1]
            };
        }
    | "NULL"
        {$$ = {type: 'null', arguments: []};}
    ;

integer-literal
    : "INTEGER_LITERAL"
        {$$ = {
            type: 'integer-literal',
            arguments: [$1]
            };
        }
    ;
%%