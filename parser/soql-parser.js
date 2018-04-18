// mygenerator.js
var Parser = require("jison").Parser;

// a grammar in JSON
var grammar = {
    "lex": {
        "rules": [
           ["\\s+", "/* skip whitespace */"],
           ["SELECT", "return 'SELECT';"],
           ["[0-9]+", "return 'NUMBER';"],
           [":\\*", "return ':*';"],
           ["$", "return 'EOF';"]
        ]
    },

    "bnf": {
        "query": ["select EOF"],
        "select": [ "SELECT select-list"],
        "select-list": [ "system-star"],
        "system-star": [":*"]
    }
};

// `grammar` can also be a string that uses jison's grammar format
var parser = new Parser(grammar);

// generate source, ready to be written to disk
var parserSource = parser.generate();

// you can also use the parser directly from memory

// returns true
parser.parse("SELECT :*");

// throws lexical error
parser.parse("SELECT *");
