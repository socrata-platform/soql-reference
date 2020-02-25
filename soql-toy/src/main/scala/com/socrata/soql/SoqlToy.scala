package com.socrata.soql

import com.socrata.soql.ast.Select
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.types._
import environment.{ColumnName, DatasetContext, ResourceName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.toy.Compat

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  val context = ResourceName("hello")

  implicit val datasetCtx = Map(
    context -> new DatasetContext[SoQLType] {
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
    },
    ResourceName("world") -> new DatasetContext[SoQLType] {
      private implicit def ctx = this
      val locale = com.ibm.icu.util.ULocale.ENGLISH
      val schema = com.socrata.soql.collection.OrderedMap(
        ColumnName(":id") -> SoQLID,
        ColumnName(":updated_at") -> SoQLFixedTimestamp,
        ColumnName(":created_at") -> SoQLFixedTimestamp,
        ColumnName(":version") -> SoQLVersion,
        ColumnName("name_last") -> SoQLText,
        ColumnName("name_first") -> SoQLText,
        ColumnName("extra_info") -> SoQLText
      )
    })

  def menu() {
    println("Columns:")
    Util.printList(datasetCtx(context).schema)
  }

  def apply(args: Array[String]) {
    menu()

    def tableFinder(in: Set[ResourceName]): Map[ResourceName, DatasetContext[SoQLType]] =
      datasetCtx.filterKeys(in)

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo, tableFinder)

    while(true) {
      val selection = Option(Compat.readLine("> ")).getOrElse("")
      if(selection == "") return;
      if(selection == "?") {
        menu()
      } else {
        try {
          val analyses = analyzer.analyzeFullQuery(context, selection)

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
          case e: SoQLException => println(e.getMessage)
        }
      }
    }
  }
}
