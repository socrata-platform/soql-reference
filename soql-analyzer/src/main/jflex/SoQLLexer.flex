package com.socrata.soql.parsing;

import scala.util.parsing.input.Position;

import com.socrata.soql.tokens.*;

import com.socrata.soql.exceptions.UnexpectedEscape;
import com.socrata.soql.exceptions.BadUnicodeEscapeCharacter;
import com.socrata.soql.exceptions.UnicodeCharacterOutOfRange;
import com.socrata.soql.exceptions.UnexpectedCharacter;
import com.socrata.soql.exceptions.UnexpectedEOF;
import com.socrata.soql.exceptions.UnterminatedString;

%%

%class Lexer
%final
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

  public Lexer(String s) {
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
%}

WhiteSpace = [ \t\n]

Identifier = ([:jletter:]|\u00ae) [:jletterdigit:]*
SystemIdentifier = ":" [:jletterdigit:]+
QuotedIdentifier = ("-" | [:jletter:]) ("-" | [:jletterdigit:])*
QuotedSystemIdentifier = ":" ("-" | [:jletterdigit:])+

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
  // Subscripting
  "."   { return token(new DOT()); }
  "["   { return token(new LBRACKET()); }
  "]"   { return token(new RBRACKET()); }

  // Math
  "+"   { return token(new PLUS()); }
  "-"   { return token(new MINUS()); }
  "*"   { return token(new STAR()); }
  "/"   { return token(new SLASH()); }
  "~"   { return token(new TILDE()); }

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
  <<EOF>> { throw new UnexpectedEOF(pos()); }
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
  <<EOF>> { throw new UnexpectedEOF(pos()); }
}

<BADQUOTEDIDENTIFIER> {
  <<EOF>> { throw new UnexpectedEOF(pos()); }
}

<SINGLEQUOTESTRING> {
  "'"     { yybegin(YYINITIAL); return finishString(); }
  "''"    { string.append('\''); }
  [^']+   { string.append(yytext()); }
  <<EOF>> { throw new UnexpectedEOF(pos()); }
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
  \\(.|\n)     { throw new UnexpectedEscape(yytext().charAt(1), pos()); }
  \n           { throw new UnterminatedString(pos()); }
  <<EOF>>      { throw new UnexpectedEOF(pos()); }
}

<ESCAPEDUNICODE> {
  [0-9a-fA-F]{4} {
    string.append((char) dehex(yytext()));
    yybegin(DOUBLEQUOTESTRING);
  }
  [0-9a-fA-F]{1,3} { yybegin(PARTIALESCAPEDUNICODE); }
  (.|\n)  { throw new BadUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw new UnexpectedEOF(pos()); }
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
  [0-9a-fA-F]{6} { throw new UnicodeCharacterOutOfRange(dehex(yytext()), pos()); }

  [0-9a-fA-F]{1,5} { yybegin(PARTIALESCAPEDUNICODE); }
  (.|\n)  { throw new BadUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw new UnexpectedEOF(pos()); }
}

<PARTIALESCAPEDUNICODE> {
  (.|\n)  { throw new BadUnicodeEscapeCharacter(yytext().charAt(0), pos()); }
  <<EOF>> { throw new UnexpectedEOF(pos()); }
}

// Fallback: captures anything that managed to escape one of the above states
.|\n { throw new UnexpectedCharacter(yytext().charAt(0), pos()); }
