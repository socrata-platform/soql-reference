// mygenerator.js
var Parser = require("jison").Parser;

// a grammar in JSON
var grammar = {
    "lex": {
        "rules": [
           ["\\s+", "/* skip whitespace */"],
           ["SELECT", "return 'SELECT';"],
           ["[0-9]+", "return 'NUMBER';"],
           [":\\*", "return ':*';"]
        ]
    },

    "bnf": {
        "program": [["select EOF", "return $1"]],
        "select": [ ["SELECT selectList", "return $2"]],
        "selectList": [ ["systemStar", "return $1"] ],
        "systemStar": [[":*", "return $1"]]
    }
};

// `grammar` can also be a string that uses jison's grammar format
var parser = new Parser(grammar);

// generate source, ready to be written to disk
var parserSource = parser.generate();

// you can also use the parser directly from memory

// returns true
parser.parse("SELECT THING");

// throws lexical error
parser.parse("SELECT *");
console.log("hi")