package com.socrata.soql

object Main extends App {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  if(args.length == 0) {
    fail("Usage: soqltoy {parse|alias|soql} ARGS...")
  }

  val op = args(0) match {
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
    case other =>
      fail("soqltoy: unknown command " + other)
  }

  op(args.drop(1))
}
