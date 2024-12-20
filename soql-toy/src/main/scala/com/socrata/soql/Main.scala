package com.socrata.soql

object Main extends App {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  if(args.length == 0) {
    fail("Usage: soqltoy [-v] {parse|alias|soql|soql2|rollup} ARGS...")
  }

  var a = args
  if(a.head == "-v") {
    System.setProperty("org.slf4j.simpleLogger.log.com.socrata.soql.analyzer2", "debug")
    a = a.tail
  }

  val op = a(0) match {
    case "parse" =>
      ParserToy
    case "alias" =>
      AliasToy
      /*
    case "typecheck" =>
      TypecheckToy
      */
    case "soql" =>
      SoqlToy
    case "soql2" =>
      analyzer2.Soql2Toy
    case "rollup" =>
      analyzer2.RollupToy
    case other =>
      fail("soqltoy: unknown command " + other)
  }

  op(a.drop(1))
}
