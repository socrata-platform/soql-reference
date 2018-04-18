%lex

%%
\s+      /* skip whitespace */


"AND"     return 'AND';

"ASC"     return 'ASC';
"BETWEEN" return 'BETWEEN';
"BY"      return 'BY';
"DESC"    return 'DESC';
"EXCEPT"  return 'EXCEPT';
"FIRST"   return 'FIRST';
"GROUP"   return 'GROUP';
"IN"      return 'IN';
"IS"      return 'IS';
"LAST"    return 'LAST';
"LIMIT"   return 'LIMIT';
"NOT"     return 'NOT';
"NULL"    return 'NULL';
"OFFSET"  return 'OFFSET';
"OR"      return 'OR';
"ORDER"   return 'ORDER';
"SELECT"  return 'SELECT';
"OR"      return 'OR';
"NOT"     return 'NOT';
":*"   return ':*';
"*"     return '*';
","     return ',';
[0-9a-zA-Z]+   return 'USER_IDENTIFIER';
:[0-9a-zA-Z]+  return 'SYSTEM_IDENTIFIER';

$      return 'EOF';

/lex

%start query
%%
query
    : select EOF
    ;

select
    : "SELECT" select-list
    ;

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

value
    : identifier-or-funcall
    | literal
    | "(" expression ")"
    ;

identifier-or-funcall
    : identifier
    ;

identifier
    : "USER_IDENTIFIER"
    | "SYSTEM_IDENTIFIER"
    ;

literal
    : "NULL"
    ;

user-identifier
    : "USER_IDENTIFIER"
    ;

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
    : "BLAH"
    ;
%%