package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag}

import com.socrata.soql.environment.{ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.analyzer2

class UnparsedFoundTables[MT <: MetaTypes] private[analyzer2] (
  private[analyzer2] val tableMap: UnparsedTableMap[MT],
  private[analyzer2] val initialScope: types.ResourceNameScope[MT],
  private[analyzer2] val initialQuery: UnparsedFoundTables.Query[MT],
  private[analyzer2] val parserParameters: EncodableParameters
) extends FoundTablesLike[MT] {
  type Self[MT <: MetaTypes] = UnparsedFoundTables[MT]

  final def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => types.DatabaseTableName[MT2],
    columnName: (DatabaseTableName, DatabaseColumnName) => types.DatabaseColumnName[MT2]
  )(implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): UnparsedFoundTables[MT2] =
    new UnparsedFoundTables(
      tableMap.rewriteDatabaseNames(tableName, columnName),
      changesOnlyLabels.convertRNS(initialScope),
      initialQuery.changeLabels[MT2],
      parserParameters
    )

  def allTableDescriptions = tableMap.allTableDescriptions
}

object UnparsedFoundTables {
  sealed abstract class Query[MT <: MetaTypes] {
    private[analyzer2] type SoQL <: String
    private[analyzer2] def soql: SoQL
    private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, FoundTables.Query[MT]]
    private[analyzer2] def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): Query[MT2]
  }

  case class Saved[MT <: MetaTypes](name: ResourceName) extends Query[MT] {
    private[analyzer2] type SoQL = Nothing
    private[analyzer2] def soql = ???

    private[analyzer2] def parse(params: AbstractParser.Parameters) = Right(FoundTables.Saved(name))
    private[analyzer2] def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): Saved[MT2] =
      this.asInstanceOf[Saved[MT2]] // SAFETY: we don't care about _anything_ in MT
  }
  object Saved {
    private[analyzer2] implicit def encode[MT <: MetaTypes] = AutomaticJsonEncodeBuilder[Saved[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes] = AutomaticJsonDecodeBuilder[Saved[MT]]
  }

  case class InContext[MT <: MetaTypes](parent: ResourceName, soql: String, parameters: Map[HoleName, MT#ColumnType]) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContext(parent, tree, soql, parameters)
      }
    private[analyzer2] def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): InContext[MT2] =
      this.asInstanceOf[InContext[MT2]] // SAFETY: we only care about CT, which isn't changing
  }
  object InContext {
    private[analyzer2] implicit def encode[MT <: MetaTypes](implicit encCT : JsonEncode[MT#ColumnType]) = AutomaticJsonEncodeBuilder[InContext[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes](implicit decCT : JsonDecode[MT#ColumnType]) = AutomaticJsonDecodeBuilder[InContext[MT]]
  }

  case class InContextImpersonatingSaved[MT <: MetaTypes](parent: ResourceName, soql: String, parameters: Map[HoleName, MT#ColumnType], fake: CanonicalName) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContextImpersonatingSaved(parent, tree, soql, parameters, fake)
      }
    private[analyzer2] def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): InContextImpersonatingSaved[MT2] =
      this.asInstanceOf[InContextImpersonatingSaved[MT2]] // SAFETY: we only care about CT, which isn't changing
  }
  object InContextImpersonatingSaved {
    private[UnparsedFoundTables] implicit def encode[MT <: MetaTypes](implicit encCT: JsonEncode[MT#ColumnType]) = AutomaticJsonEncodeBuilder[InContextImpersonatingSaved[MT]]
    private[UnparsedFoundTables] implicit def decode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#ColumnType]) = AutomaticJsonDecodeBuilder[InContextImpersonatingSaved[MT]]
  }

  case class Standalone[MT <: MetaTypes](soql: String, parameters: Map[HoleName, MT#ColumnType]) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.Standalone(tree, soql, parameters)
      }
    private[analyzer2] def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, MT2]): Standalone[MT2] =
      this.asInstanceOf[Standalone[MT2]] // SAFETY: we only care about CT, which isn't changing
  }
  object Standalone {
    private[UnparsedFoundTables] implicit def encode[MT <: MetaTypes](implicit encCT: JsonEncode[MT#ColumnType]) = AutomaticJsonEncodeBuilder[Standalone[MT]]
    private[UnparsedFoundTables] implicit def decode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#ColumnType]) = AutomaticJsonDecodeBuilder[Standalone[MT]]
  }

  object Query {
    private[analyzer2] implicit def queryEncode[MT <: MetaTypes](implicit encCT : JsonEncode[MT#ColumnType]): JsonEncode[Query[MT]] = SimpleHierarchyEncodeBuilder[Query[MT]](InternalTag("type")).
      branch[Saved[MT]]("saved").
      branch[InContext[MT]]("in-context").
      branch[InContextImpersonatingSaved[MT]]("in-context-impersonating-saved").
      branch[Standalone[MT]]("standalone").
      build

    private[analyzer2] implicit def queryDecode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#ColumnType]): JsonDecode[Query[MT]] = SimpleHierarchyDecodeBuilder[Query[MT]](InternalTag("type")).
      branch[Saved[MT]]("saved").
      branch[InContext[MT]]("in-context").
      branch[InContextImpersonatingSaved[MT]]("in-context-impersonating-saved").
      branch[Standalone[MT]]("standalone").
      build
  }

  implicit def jEncode[MT <: MetaTypes](implicit rnsEncode: JsonEncode[MT#ResourceNameScope], ctEncode: JsonEncode[MT#ColumnType], dtnEncode: JsonEncode[MT#DatabaseTableNameImpl], dcnEncode: JsonEncode[MT#DatabaseColumnNameImpl]) =
    AutomaticJsonEncodeBuilder[UnparsedFoundTables[MT]]
  implicit def jDecode[MT <: MetaTypes](implicit rnsDecode: JsonDecode[MT#ResourceNameScope], ctDecode: JsonDecode[MT#ColumnType], dtnDecode: JsonDecode[MT#DatabaseTableNameImpl], dcnDecode: JsonDecode[MT#DatabaseColumnNameImpl]) =
    AutomaticJsonDecodeBuilder[UnparsedFoundTables[MT]]
}

