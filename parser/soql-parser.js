// mygenerator.js
var fs = require("fs");
var Parser = require("jison").Parser;

var bnf = fs.readFileSync("parser/soql.y", "utf8");
var parser = new Parser(bnf);

// parser.parse ("SELECT *");
// parser.parse("SELECT :*, *");
// parser.parse("SELECT thing1, thing2");

// parser.parse("SELECT 1 + 1 WHERE 1 = 1");
// parser.parse("SELECT 1 + 1 WHERE 1 == 1");
// var result = parser.parse("SELECT 1 + -1 WHERE 1 == 1");
// console.log(result);

var examples = [
  "SELECT :*",
  "SELECT *",
  "SELECT :*, *",
  "SELECT cat AS kitten",
  "SELECT :*, cat AS kitten",
  "SELECT :*, *, cat AS kitten",
  "SELECT count(*)",
  "SELECT count(cat) AS catcount",
  "SELECT * ORDER BY cat",
  "SELECT * ORDER BY cat DESC NULL FIRST",
  "SELECT * LIMIT 10"
];

for (index in examples) {
  var select = examples[index];
  console.log(select);
  console.log(JSON.stringify(
    parser.parse(select), null, 2)
  );
  console.log();
}