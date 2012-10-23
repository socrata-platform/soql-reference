package com.socrata.soql

import com.socrata.soql.aliases._

import com.socrata.soql.names.ColumnName
import com.socrata.soql.parsing.Parser
import com.socrata.soql.exceptions.SoQLException

object AliasToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = new UntypedDatasetContext {
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
        case e: SoQLException => println(e.getMessage)
      }
    }
  }
}
