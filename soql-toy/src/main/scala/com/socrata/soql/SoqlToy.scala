package com.socrata.soql

import ast.Select
import com.socrata.soql.analysis._
import com.socrata.soql.analysis.types._

import com.socrata.soql.names.ColumnName
import com.socrata.soql.parsing.{BadParseException, LexerError, Parser}
import com.socrata.collection.OrderedMap

object SoqlToy extends (Array[String] => Unit) {
  def fail(msg: String) = {
    System.err.println(msg)
    sys.exit(1)
  }

  implicit val datasetCtx = new DatasetContext {
    implicit val ctx = this
    val locale = com.ibm.icu.util.ULocale.ENGLISH
    val columnTypes = com.socrata.collection.OrderedMap(
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

    val columns = columnTypes.keySet
  }

  def menu() {
    println("Columns:")
    for((k,v) <- datasetCtx.columnTypes) {
      println("  " + k + ("." * (15 - k.toString.length)) + v)
    }
  }

  def apply(args: Array[String]) {
    menu()
    val p = new Parser
    while(true) {
      val selection = readLine("> ")
      if(selection == null) return;
      if(selection == "?") {
        menu()
      } else {
        try {
          val start = System.nanoTime()

          val ast: Select = p.selectStatement(selection)

          val afterParse = System.nanoTime()

          val aliasesUntyped = AliasAnalysis(ast.selection)

          val afterAliasAnalysis = System.nanoTime()

          val e2e = aliasesUntyped.evaluationOrder.foldLeft(new EndToEnd(OrderedMap.empty, datasetCtx.columnTypes)) { (e2e, alias) =>
            val r = e2e(aliasesUntyped.expressions(alias))
            new EndToEnd(e2e.aliases + (alias -> r), datasetCtx.columnTypes)
          }

          val afterAliasTypechecking = System.nanoTime()

          val aliases = e2e.aliases
          val where = ast.where.map(e2e)

          val afterWhereTypechecking = System.nanoTime()

          val groupBys = ast.groupBy.map(_.map(e2e))

          val afterGroupByTypechecking = System.nanoTime()

          val having = ast.having.map(e2e)

          val afterHavingTypechecking = System.nanoTime()

          val orderBys = ast.orderBy.map { obs => obs.zip(obs.map { ob => e2e(ob.expression) }) }

          val afterOrderByTypechecking = System.nanoTime()

          val aggregateChecker = new AggregateChecker[SoQLType]
          val hasAggregates = aggregateChecker(aliases.values.toSeq, where, groupBys, having, orderBys.getOrElse(Nil).map(_._2))

          val afterAggregateChecking = System.nanoTime()

          val end = System.nanoTime()

          println("Outputs:")
          for((k,v) <- aliases) {
            println("  " + k + ("." * math.max(1, 15 - k.toString.length)) + v)
          }
          where.foreach { w =>
            println("where:\n  " + w)
          }
          groupBys.foreach { gbs =>
            println("group bys:")
            for(gb <- gbs) {
              println("  " + gb)
            }
          }
          having.foreach { h =>
            println("having:\n  " + h)
          }
          orderBys.map { obs =>
            println("order bys:")
            for((ob, e) <- obs) {
              println("  " + e + " (" + (if(ob.ascending) "ascending" else "descending") + ", nulls " + (if(ob.nullLast) "last" else "first") + ")")
            }
          }
          println("has aggregates: " + hasAggregates)
          val timings = OrderedMap("parse" -> (afterParse - start), "alias" -> (afterAliasAnalysis - afterParse), "aliasType" -> (afterAliasTypechecking - afterAliasAnalysis), "where" -> (afterWhereTypechecking - afterAliasTypechecking), "groupBy" -> (afterGroupByTypechecking - afterWhereTypechecking), "having" -> (afterHavingTypechecking - afterGroupByTypechecking), "orderBy" -> (afterOrderByTypechecking - afterHavingTypechecking), "aggregate" -> (afterAggregateChecking - afterOrderByTypechecking))
          for((k, v) <- timings) {
            println("after " + k + ": " + (v / 1000000))
          }
          println("Total analysis time: " + ((end - start) / 1000000) + "ms")
        } catch {
          case e: LexerError => println(e.getMessage)
          case e: BadParseException => println(e.getMessage)
          case e: RepeatedExceptionException => println(e.getMessage)
          case e: NoSuchColumnException => println(e.getMessage)
          case e: TypeCheckException => println(e.getMessage)
          case e: DuplicateAliasException => println(e.getMessage)
          case e: CircularAliasDefinitionException => println(e.getMessage)
          case e: TypeMismatchError[_] => println(e.getMessage)
          case e: AmbiguousCall => println(e.getMessage)
          case e: ColumnNotInGroupBys => println(e.getMessage)
          case e: AggregateInUngroupedContext => println(e.getMessage)
        }
      }
    }
  }
}
