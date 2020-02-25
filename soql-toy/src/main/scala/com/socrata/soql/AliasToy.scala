package com.socrata.soql

import collection.OrderedSet
import com.socrata.soql.aliases._
import com.socrata.soql.parsing.Parser
import com.socrata.soql.exceptions.SoQLException
import environment.{ColumnName, ResourceName}
import com.socrata.soql.toy.Compat

object AliasToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  val context = ResourceName("hello")

  implicit val datasetCtx = Map((Some(context) : Option[ResourceName]) ->
                                  com.socrata.soql.collection.OrderedSet(":id", ":created_at", "a", "b", "c", "d").map(ColumnName(_)))

  def menu() {
    println("Columns:")
    println(datasetCtx(Some(context)).mkString("  ", ", ", ""))
  }

  def apply(args: Array[String]) {
    menu()
    val p = new Parser
    while(true) {
      val selection = Compat.readLine("> ")
      if(selection == null) return;
      try {
        val analysis = AliasAnalysis(datasetCtx, p.selection(selection))
        println("Resulting aliases:")
        Util.printList(analysis.expressions, "  ")
        println(analysis.evaluationOrder.mkString("Typecheck in this order:\n  ", ", ", ""))
      } catch {
        case e: SoQLException => println(e.getMessage)
      }
    }
  }
}
