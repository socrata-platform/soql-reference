package com.socrata.soql.analyzer2

import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.soql.environment.{ColumnName, ResourceName, Provenance}
import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo, SoQLFunctions, MonomorphicFunction}
import com.socrata.soql.types._
import com.socrata.soql.util.SoQLErrorCodec

object AnalyzeFoundTablesHelper {
  trait MT extends MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = (ResourceName, String)
    type DatabaseColumnNameImpl = ColumnName
  }
}

object AnalyzeFoundTables extends (Array[String] => Unit) with StatementUniverse[AnalyzeFoundTablesHelper.MT] {
  import AnalyzeFoundTablesHelper._

  val provenanceMapper = new ToProvenance with FromProvenance {
    def fromProvenance(dtn: Provenance): DatabaseTableName = {
      val Right(name) = JsonUtil.parseJson[(ResourceName, String)](dtn.value)
      DatabaseTableName(name)
    }
    def toProvenance(dtn: DatabaseTableName): Provenance = Provenance(JsonUtil.renderJson(dtn.name, pretty = false))
  }

  val analyzer = new SoQLAnalyzer[MT](SoQLTypeInfo.soqlTypeInfo2, SoQLFunctionInfo, provenanceMapper)

  def apply(argv: Array[String]): Unit = {
    val Right(foundTables) = JsonUtil.readJsonFile[FoundTables[MT]](argv(0), StandardCharsets.UTF_8)
    analyzer(
      foundTables,
      UserParameters.emptyFor(foundTables)
    ) match {
      case Right(_) =>
        println("Good!")
      case Left(err) =>
        implicit val errorCodecs = SoQLError.errorCodecs[Int, SoQLError[Int]]().build
        println(errorCodecs.encode(err))
    }
  }
}
