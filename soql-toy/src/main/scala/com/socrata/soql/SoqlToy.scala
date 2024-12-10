package com.socrata.soql

import com.socrata.soql.ast.{Select, UDF}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.types._
import environment.{ColumnName, DatasetContext, TableName, HoleName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.soql.parsing.{Parser, AbstractParser}
import scala.io.StdIn.readLine

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = AnalysisContext[SoQLType, SoQLValue](
    schemas = Map(TableName.PrimaryTable.qualifier -> new DatasetContext[SoQLType] {
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
                      ColumnName("balance") -> SoQLNumber,
                      ColumnName("object") -> SoQLJson,
                      ColumnName("array") -> SoQLJson,
                      ColumnName("dbl") -> SoQLDouble,
                      ColumnName(":@meta") -> SoQLJson
                    )
                  }),
      parameters = ParameterSpec(
        parameters = Map("aaaa-aaaa" -> Map(
                           HoleName("hello") -> PresentParameter(SoQLText("world")),
                           HoleName("goodbye") -> MissingParameter(SoQLNumber.t)
                         )),
        default = "aaaa-aaaa"
      )
  )

  def menu(): Unit = {
    println("Columns:")
    Util.printList(datasetCtx.schemas(TableName.PrimaryTable.qualifier).schema)
  }

  def apply(args: Array[String]): Unit = {
    menu()

    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

    val stored_procs = Map(
      TableName("_is_admin") -> UDF(
        arguments = Seq(HoleName("fn") -> SoQLText.name,
                        HoleName("ln") -> SoQLText.name),
        body = new Parser(AbstractParser.defaultParameters.copy(allowHoles = true)).binaryTreeSelect("select 1 where ?fn = 'Adam' and ?ln = 'Admin'")
      ),
      TableName("_positive_balance") -> UDF(
        arguments = Seq(HoleName("balance") -> SoQLNumber.name),
        body = new Parser(AbstractParser.defaultParameters.copy(allowHoles = true)).binaryTreeSelect("select 1 where ?balance > 0")
      )
    )

    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      if(selection == "?") {
        menu()
      } else if(selection == "exit" || selection == "quit") {
        return
      } else {
        try {
          val parsed = new Parser(AbstractParser.defaultParameters.copy(allowJoinFunctions = true)).binaryTreeSelect(selection)
          val substituted = Select.rewriteJoinFuncs(parsed, stored_procs)
          println(substituted)
          val analyses = analyzer.analyzeFullQuery(substituted.toString)

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
