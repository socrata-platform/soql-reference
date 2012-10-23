package com.socrata.soql

import com.socrata.soql.aggregates.AggregateChecker
import com.socrata.soql.aliases.AliasAnalysis
import com.socrata.soql.ast.Select
import com.socrata.soql.types._
import com.socrata.soql.names.ColumnName
import com.socrata.soql.parsing.Parser
import com.socrata.collection.OrderedMap

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = new DatasetContext[SoQLType] {
    private implicit def ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val schema = com.socrata.collection.OrderedMap(
      ColumnName(":id") -> SoQLNumber,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation,
      ColumnName("balance") -> SoQLMoney
    )
  }

  def menu() {
    println("Columns:")
    for((k,v) <- datasetCtx.schema) {
      println("  " + k + ("." * (15 - k.toString.length)) + v)
    }
  }

  def apply(args: Array[String]) {
    menu()

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo)

    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      if(selection == "?") {
        menu()
      } else {
        try {
          val analysis = analyzer.analyzeFullQuery(selection)

          println("Outputs:")
          for((k,v) <- analysis.selection) {
            println("  " + k + ("." * math.max(1, 15 - k.toString.length)) + v)
          }
          analysis.where.foreach { w =>
            println("where:\n  " + w)
          }
          analysis.groupBy.foreach { gbs =>
            println("group bys:")
            for(gb <- gbs) {
              println("  " + gb)
            }
          }
          analysis.having.foreach { h =>
            println("having:\n  " + h)
          }
          analysis.orderBy.map { obs =>
            println("order bys:")
            for(ob <- obs) {
              println("  " + ob.expression + " (" + (if(ob.ascending) "ascending" else "descending") + ", nulls " + (if(ob.nullLast) "last" else "first") + ")")
            }
          }
          println("has aggregates: " + analysis.isGrouped)
        } catch {
          case e: SoQLException => println(e.getMessage)
        }
      }
    }
  }
}
