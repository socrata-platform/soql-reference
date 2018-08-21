package com.socrata.soql

import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.types._
import environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = Map(TableName.PrimaryTable.name -> new DatasetContext[SoQLType] {
    private implicit def ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val schema = com.socrata.soql.collection.OrderedMap(
      ColumnName(":id") -> SoQLID,
      ColumnName(":updated_at") -> SoQLFixedTimestamp,
      ColumnName(":created_at") -> SoQLFixedTimestamp,
      ColumnName(":version") -> SoQLVersion,
      ColumnName("name_last") -> SoQLText,
      ColumnName("name_first") -> SoQLText,
      ColumnName("visits") -> SoQLNumber,
      ColumnName("last_visit") -> SoQLFixedTimestamp,
      ColumnName("address") -> SoQLLocation,
      ColumnName("balance") -> SoQLMoney,
      ColumnName("object") -> SoQLObject,
      ColumnName("array") -> SoQLArray,
      ColumnName("dbl") -> SoQLDouble,
      ColumnName(":@meta") -> SoQLObject
    )
  })

  def menu() {
    println("Columns:")
    Util.printList(datasetCtx(TableName.PrimaryTable.name).schema)
  }

  def apply(args: Array[String]) {
    menu()

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      if(selection == "?") {
        menu()
      } else {
        try {
          val analyses = analyzer.analyzeFullQuery(selection)

          println("Outputs:")
          analyses.foreach { analysis =>
            Util.printList(analysis.selection)
            analysis.where.foreach { w =>
              println("where:\n  " + w)
            }
            analysis.groupBy.foreach { gbs =>
              println("group bys:")
              for (gb <- gbs) {
                println("  " + gb)
              }
            }
            analysis.having.foreach { h =>
              println("having:\n  " + h)
            }
            analysis.orderBy.map { obs =>
              println("order bys:")
              for (ob <- obs) {
                println("  " + ob.expression + " (" + (if (ob.ascending) "ascending" else "descending") + ", nulls " + (if (ob.nullLast) "last" else "first") + ")")
              }
            }
            println("has aggregates: " + analysis.isGrouped)
          }
        } catch {
          case e: SoQLException => println(e.getMessage)
        }
      }
    }
  }
}
