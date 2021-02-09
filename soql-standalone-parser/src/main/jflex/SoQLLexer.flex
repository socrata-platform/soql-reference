package com.socrata.soql.parsing;

import scala.util.parsing.input.Position;

import com.socrata.soql.tokens.*;

%%

%class AbstractLexer
%abstract
%type Token
%unicode
%char
%line
%column
%caseless

%{
  String sourceString;
  StringBuilder string = new StringBuilder();
  Position stringStart;

  Token finishString() {
    Token s = new StringLiteral(string.toString());
    s.setPosition(stringStart);
    return s;
  }

  int dehex(String s) {
    return Integer.parseInt(s, 16);
  }

  public AbstractLexer(String s) {
    this(new java.io.StringReader(s));
    sourceString = s;
  }

  public Position pos() {
    return new SoQLPosition(1 + yyline, 1 + yycolumn, sourceString, yychar);
  }

  public Token token(Token t) {
    t.setPosition(pos());
    return t;
  }

  public java.util.List<Token> all() throws java.io.IOException {
    java.util.ArrayList<Token> results = new java.util.ArrayList<Token>();
    for(Token t = yylex(); t != null; t = yylex()) results.add(t);
    return results;
  }

  protected abstract RuntimeException unexpectedEOF(Position pos);
  protected abstract RuntimeException unexpectedEscape(char c, Position pos);
  protected abstract RuntimeException unterminatedString(Position pos);
  protected abstract RuntimeException badUnicodeEscapeCharacter(char c, Position pos);
  protected abstract RuntimeException unicodeCharacterOutOfRange(int codepoint, Position pos);
  protected abstract RuntimeException unexpectedCharacter(char c, Position pos);
%}

WhiteSpace = [ \t\r\n]

Identifier = [:jletter:] [:jletterdigit:]*
SystemIdentifier = ":" "@"? [:jletterdigit:]+
QuotedIdentifier = ("-" | [:jletter:]) ("-" | [:jletterdigit:])*
QuotedSystemIdentifier = ":" "@"? ("-" | [:jletterdigit:])+

TableIdentifier = "@" ("-" | [:jletterdigit:])+

%state QUOTEDIDENTIFIER
%state QUOTEDSYSTEMIDENTIFIER
%state BADQUOTEDIDENTIFIER
%state SINGLEQUOTESTRING
%state DOUBLEQUOTESTRING
%state ESCAPEDUNICODE
%state ESCAPEDUNICODE6
%state PARTIALESCAPEDUNICODE

%%

<YYINITIAL> {
  // Query chaining
  "|>"  { return token(new QUERYPIPE()); }

  "UNION" { return token(new QUERYUNION()); }
  "INTERSECT"  { return token(new QUERYINTERSECT()); }
  "MINUS"  { return token(new QUERYMINUS()); }

  "UNION" {WhiteSpace}+ "ALL" { return token(new QUERYUNIONALL()); }
  "INTERSECT" {WhiteSpace}+ "ALL" { return token(new QUERYINTERSECTALL()); }
  "MINUS" {WhiteSpace}+ "ALL" { return token(new QUERYMINUSALL()); }

  // Subscripting
  // "." share with qualifying
  "["   { return token(new LBRACKET()); }
  "]"   { return token(new RBRACKET()); }

  // Qualifying
  "."   { return token(new DOT()); }

  // Math
  "+"   { return token(new PLUS()); }
  "-"   { return token(new MINUS()); }
  "*"   { return token(new STAR()); }
  "/"   { return token(new SLASH()); }
  "~"   { return token(new TILDE()); }
  "^"   { return token(new CARET()); }
  "%"   { return token(new PERCENT()); }

  // Misc expression-y stuf
  "||"  { return token(new PIPEPIPE()); }
  "::"  { return token(new COLONCOLON()); }
  "("   { return token(new LPAREN()); }
  ")"   { return token(new RPAREN()); }

  // Comparisons
  "<"   { return token(new LESSTHAN()); }
  "<="  { return token(new LESSTHANOREQUALS()); }
  "="   { return token(new EQUALS()); }
  "=="  { return token(new EQUALSEQUALS()); }
  ">="  { return token(new GREATERTHANOREQUALS()); }
  ">"   { return token(new GREATERTHAN()); }
  "<>"  { return token(new LESSGREATER()); }
  "!="  { return token(new BANGEQUALS()); }

  "!"   { return token(new BANG()); }

  [0-9]+ { return token(new IntegerLiteral(scala.math.BigInt.apply(yytext()))); }
  [0-9]+(\.[0-9]*)?(e[+-]?[0-9]+)? { return token(new NumberLiteral(new java.math.BigDecimal(yytext()))); }

  "'"  { string.setLength(0); stringStart = pos(); yybegin(SINGLEQUOTESTRING); }
  "\"" { string.setLength(0); stringStart = pos(); yybegin(DOUBLEQUOTESTRING); }

  // Identifiers
  {Identifier} { return token(new Identifier(yytext(), false)); }
  "`" { stringStart = pos(); yybegin(QUOTEDIDENTIFIER); }

  // Identifiers
  {SystemIdentifier} { return token(new SystemIdentifier(yytext(), false)); }
  "`" / ":" { stringStart = pos(); yybegin(QUOTEDSYSTEMIDENTIFIER); }

  {TableIdentifier} { return token(new TableIdentifier(yytext())); }

  // Punctuation
  ","  { return token(new COMMA()); }
  ":*" { return token(new COLONSTAR()); }

  {WhiteSpace} { }

  "--" .* { /* comment */ }

  <<EOF>> { return token(new EOF()); }
}

<QUOTEDIDENTIFIER> {
  {QuotedIdentifier} "`"     {
    String t = yytext();
    Token i = new Identifier(t.substring(0, t.length() - 1), true);
    i.setPosition(stringStart);
    yybegin(YYINITIAL);
    return i;
  }
  {QuotedIdentifier} { yybegin(BADQUOTEDIDENTIFIER); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<QUOTEDSYSTEMIDENTIFIER> {
  {QuotedSystemIdentifier} "`"     {
    String t = yytext();
    Token i = new SystemIdentifier(t.substring(0, t.length() - 1), true);
    i.setPosition(stringStart);
    yybegin(YYINITIAL);
    return i;
  }
  {QuotedSystemIdentifier} { yybegin(BADQUOTEDIDENTIFIER); }
  ":" { yybegin(BADQUOTEDIDENTIFIER); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<BADQUOTEDIDENTIFIER> {
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<SINGLEQUOTESTRING> {
  "'"     { yybegin(YYINITIAL); return finishString(); }
  "''"    { string.append('\''); }
  [^']+   { string.append(yytext()); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<DOUBLEQUOTESTRING> {
  \"           { yybegin(YYINITIAL); return finishString(); }
  [^\n\"\\]+   { string.append(yytext()); }
  \\[n]        { string.append('\n'); }
  \\[t]        { string.append('\t'); }
  \\[b]        { string.append('\b'); }
  \\[f]        { string.append('\f'); }
  \\[r]        { string.append('\r'); }
  \\\"         { string.append('"'); }
  \\\\         { string.append('\\'); }
  \\[u]        { yybegin(ESCAPEDUNICODE); }
  \\[U]        { yybegin(ESCAPEDUNICODE6); }
  \\(.|\n)     { throw unexpectedEscape(yytext().charAt(1), pos()); }
  \n           { throw unterminatedString(pos()); }
  <<EOF>>      { throw unexpectedEOF(pos()); }
}

<ESCAPEDUNICODE> {
  [0-9a-fA-F]{4} {
    string.append((char) dehex(yytext()));
    yybegin(DOUBLEQUOTESTRING);
  }
  [0-9a-fA-F]{1,3} { yybegin(PARTIALESCAPEDUNICODE); }
  (.|\n)  { throw badUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<ESCAPEDUNICODE6> {
  // Unicode only defines up to 0x10ffff.  The first sub-regex
  // here matches 0x000000-0x0fffff and the second matches
  // 0x100000-0x10ffff.
  (0 [0-9a-fA-F]{5}) | (1 0 [0-9a-fA-F]{4}) {
    char[] chars = Character.toChars(dehex(yytext()));
    string.append(chars);
    yybegin(DOUBLEQUOTESTRING);
  }
  // Six digits of hex that didn't match the above?  Out of range!
  [0-9a-fA-F]{6} { throw unicodeCharacterOutOfRange(dehex(yytext()), pos()); }

  [0-9a-fA-F]{1,5} { yybegin(PARTIALESCAPEDUNICODE); }
  (.|\n)  { throw badUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

<PARTIALESCAPEDUNICODE> {
  (.|\n)  { throw badUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw unexpectedEOF(pos()); }
}

// Fallback: captures anything that managed to escape one of the above states
.|\n { throw unexpectedCharacter(yytext().charAt(0), pos()); }
