%lex

%%
\s+      /* skip whitespace */
"SELECT"  return 'SELECT';
":*"   return ':*';
$      return 'EOF';

/lex

%start query
%%
query:          select EOF;
select:         "SELECT" select-list;
select-list:    system-star;
system-star:    ":*";
%%