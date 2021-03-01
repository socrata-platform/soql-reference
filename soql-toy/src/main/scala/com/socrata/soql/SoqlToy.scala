package com.socrata.soql

import com.socrata.soql.ast.{Select, TableName}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.types._
import environment.{ColumnName, DatasetContext}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.rojoma.json.v3.util.JsonUtil

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = Map(TableName.PrimaryTable.qualifier -> new DatasetContext[SoQLType] {
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
    Util.printList(datasetCtx(TableName.PrimaryTable.qualifier).schema)
  }

  def apply(args: Array[String]) {
    menu()

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      if(selection == "?") {
        menu()
      } else if(selection == "exit" || selection == "quit") {
        return
      } else {
        try {
          val analyses = analyzer.analyzeFullQuery(selection)

          println("Outputs:")
          analyses.seq.foreach { analysis =>
            Util.printList(analysis.selection)
            analysis.where.foreach { w =>
              println("where:\n  " + w)
            }
            println(Select.itrToString("group bys:\n", analysis.groupBys))
            println(Select.itrToString("having:\n", analysis.having))
            val obs = analysis.orderBys
            if (obs.nonEmpty) {
              println("order bys:")
              obs.map { ob =>
                println("  " + ob.expression + " (" + (if (ob.ascending) "ascending" else "descending") + ", nulls " + (if (ob.nullLast) "last" else "first") + ")")
              }
            }
            println("has aggregates: " + analysis.isGrouped)
          }
        } catch {
          case e: SoQLException =>
            println(e.getMessage)
            println(JsonUtil.renderJson(e, pretty = true))
            println(JsonUtil.parseJson[SoQLException](JsonUtil.renderJson(e)))
        }
      }
    }
  }
}
