package com.socrata.soql

import collection.OrderedSet
import com.socrata.soql.aliases._
import com.socrata.soql.parsing.Parser
import com.socrata.soql.exceptions.SoQLException
import environment.{ColumnName, TableName, UntypedDatasetContext}

object AliasToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = Map(TableName.PrimaryTable.name -> new UntypedDatasetContext {
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val columns = com.socrata.soql.collection.OrderedSet(":id", ":created_at", "a", "b", "c", "d").map(ColumnName(_))
  })

  def menu() {
    println("Columns:")
    println(datasetCtx(TableName.PrimaryTable.name).columns.mkString("  ", ", ", ""))
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
        Util.printList(analysis.expressions, "  ")
        println(analysis.evaluationOrder.mkString("Typecheck in this order:\n  ", ", ", ""))
      } catch {
        case e: SoQLException => println(e.getMessage)
      }
    }
  }
}
