package com.socrata.soql.analyzer2

import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, JsonUtil}
import com.socrata.prettyprint.prelude._
import com.socrata.soql.environment.{ColumnName, TypeName, ResourceName, Provenance, ScopedResourceName}
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo2}
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.rollup.{RollupRewriter, RollupInfo}
import com.socrata.soql.stdlib.analyzer2.rollup.SoQLRollupExact
import com.socrata.soql.util.LazyToString

object RollupToy extends (Array[String] => Unit) {
  case class Config(
    table: Map[String, TypeName],
    rollups: Map[String, String],
    query: String
  )

  implicit val configDecode = AutomaticJsonDecodeBuilder[Config]

  trait MT extends MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
    type ResourceNameScope = Unit
  }

  val typeInfo = new SoQLTypeInfo2[MT]

  val toProvenance = new ToProvenance[String] {
    override def toProvenance(dtn: DatabaseTableName[String]): Provenance =
      Provenance(dtn.name)
  }

  val analyzer = new SoQLAnalyzer[MT](typeInfo, SoQLFunctionInfo, toProvenance)

  implicit val cvHasDoc = new HasDoc[SoQLValue] {
    def docOf(cv: SoQLValue) = Doc(cv.toString)
  }

  val stringifier = new Stringifier[MT] {
    def statement(stmt: Statement) = LazyToString(stmt.debugDoc)
    def from(from: From) = LazyToString(from.debugDoc)
    def expr(expr: Expr) = LazyToString(expr.debugDoc(implicitly, implicitly))
  }

  val rollupExact = new SoQLRollupExact[MT](stringifier)

  def apply(args: Array[String]) = {
    for(file <- args) {
      val config = JsonUtil.readJsonFile[Config](file, StandardCharsets.UTF_8) match {
        case Right(c) => c
        case Left(e) => throw new Exception(e.english)
      }

      val tf = MockTableFinder[MT](
        ((), "table") -> D(config.table.map { case (cn, tn) => cn -> typeInfo.typeFor(tn).getOrElse(throw new Exception("Unknown type " + tn)) }.toSeq : _*)
      )

      val rollups = config.rollups.iterator.map { case (name, soql) =>
        val foundTables = tf.findTables((), ResourceName("table"), soql, Map.empty) match {
          case Right(ft) => ft
          case Left(err) => throw new Exception(err.toString)
        }

        val analysis = analyzer(foundTables, UserParameters.emptyFor(foundTables)) match {
          case Right(a) => a
          case Left(err) => throw new Exception(err.toString)
        }

        new RollupInfo[MT, String] {
          override val id = name
          override val statement = analysis.statement
          override val resourceName = ScopedResourceName((), ResourceName(s"rollup:$name"))
          override val databaseName = DatabaseTableName(name)
          override def databaseColumnNameOfIndex(idx: Int) = DatabaseColumnName("c" + idx)
        }
      }.toVector

      val foundTables = tf.findTables((), ResourceName("table"), config.query, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => throw new Exception(err.toString)
      }

      val analysis = analyzer(foundTables, UserParameters.emptyFor(foundTables)) match {
        case Right(a) => a
        case Left(err) => throw new Exception(err.toString)
      }

      val rollupRewriter = new RollupRewriter[MT, String](
        analysis.labelProvider,
        rollupExact,
        rollups
      )

      println("Found rollups:")
      for((stmt, rids) <- rollupRewriter.rollup(analysis.statement)) {
        println(s"${stmt.debugDoc} -- ${rids.mkString(", ")}")
      }
    }
  }
}
