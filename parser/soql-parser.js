// mygenerator.js
var fs = require("fs");
var Parser = require("jison").Parser;

var bnf = fs.readFileSync("soql.y", "utf8");
var parser = new Parser(bnf);

// returns true
parser.parse("SELECT :*");

// throws lexical error
parser.parse("SELECT *");
