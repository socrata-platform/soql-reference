// mygenerator.js
var fs = require("fs");
var Parser = require("jison").Parser;

var bnf = fs.readFileSync("soql.y", "utf8");
var parser = new Parser(bnf);

// parser.parse("SELECT :*");
// parser.parse("SELECT *");
// parser.parse("SELECT :*, *");
// parser.parse("SELECT thing1, thing2");

console.log(JSON.stringify(
    parser.parse("SELECT blah, fdagfd, fdsag WHERE 1 + 1 == 4 ORDER BY fdsgfsd ASC")
)
);