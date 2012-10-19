package com.socrata.soql

import com.socrata.soql.analysis._

import com.socrata.soql.names.ColumnName
import com.socrata.soql.parsing.{BadParseException, LexerError, Parser}

object AliasToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = new DatasetContext {
    implicit val ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val columns = com.socrata.collection.OrderedSet(":id", ":created_at", "a", "b", "c", "d").map(ColumnName(_))
  }

  def menu() {
    println("Columns:")
    println(datasetCtx.columns.mkString("  ", ", ", ""))
  }

  def apply(args: Array[String]) {
    menu()
    val p = new Parser
    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      try {
        val analysis = AliasAnalysis(p.selection(selection))
        println("Resulting aliases:")
        for((k,v) <- analysis.expressions) {
          println("  " + k + ("." * math.max(1, 15 - k.toString.length)) + v)
        }
        println(analysis.evaluationOrder.mkString("Typecheck in this order:\n  ", ", ", ""))
      } catch {
        case e: LexerError => println(e.getMessage)
        case e: BadParseException => println(e.getMessage)
        case e: RepeatedExceptionException => println(e.getMessage)
        case e: NoSuchColumnException => println(e.getMessage)
        case e: DuplicateAliasException => println(e.getMessage)
        case e: CircularAliasDefinitionException => println(e.getMessage)
      }
    }
  }
}
