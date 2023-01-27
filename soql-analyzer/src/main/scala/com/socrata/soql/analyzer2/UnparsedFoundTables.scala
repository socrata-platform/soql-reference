package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag}

import com.socrata.soql.environment.{ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser

class UnparsedFoundTables[MT <: MetaTypes] private[analyzer2] (
  private[analyzer2] val tableMap: UnparsedTableMap[MT],
  private[analyzer2] val initialScope: MT#ResourceNameScope,
  private[analyzer2] val initialQuery: UnparsedFoundTables.Query[MT],
  private[analyzer2] val parserParameters: EncodableParameters
) extends FoundTablesLike[MT] {
  type Self[MT <: MetaTypes] = UnparsedFoundTables[MT]

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): UnparsedFoundTables[MT] =
    new UnparsedFoundTables(
      tableMap.rewriteDatabaseNames(tableName, columnName),
      initialScope,
      initialQuery,
      parserParameters
    )
}

object UnparsedFoundTables {
  sealed abstract class Query[MT <: MetaTypes] {
    private[analyzer2] type SoQL <: String
    private[analyzer2] def soql: SoQL
    private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, FoundTables.Query[MT]]
  }

  case class Saved[MT <: MetaTypes](name: ResourceName) extends Query[MT] {
    private[analyzer2] type SoQL = Nothing
    private[analyzer2] def soql = ???

    private[analyzer2] def parse(params: AbstractParser.Parameters) = Right(FoundTables.Saved(name))
  }
  object Saved {
    private[analyzer2] implicit def encode[MT <: MetaTypes] = AutomaticJsonEncodeBuilder[Saved[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes] = AutomaticJsonDecodeBuilder[Saved[MT]]
  }

  case class InContext[MT <: MetaTypes](parent: ResourceName, soql: String, parameters: Map[HoleName, MT#CT]) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContext(parent, tree, soql, parameters)
      }
  }
  object InContext {
    private[analyzer2] implicit def encode[MT <: MetaTypes](implicit encCT : JsonEncode[MT#CT]) = AutomaticJsonEncodeBuilder[InContext[MT]]
    private[analyzer2] implicit def decode[MT <: MetaTypes](implicit decCT : JsonDecode[MT#CT]) = AutomaticJsonDecodeBuilder[InContext[MT]]
  }

  case class InContextImpersonatingSaved[MT <: MetaTypes](parent: ResourceName, soql: String, parameters: Map[HoleName, MT#CT], fake: CanonicalName) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContextImpersonatingSaved(parent, tree, soql, parameters, fake)
      }
  }
  object InContextImpersonatingSaved {
    private[UnparsedFoundTables] implicit def encode[MT <: MetaTypes](implicit encCT: JsonEncode[MT#CT]) = AutomaticJsonEncodeBuilder[InContextImpersonatingSaved[MT]]
    private[UnparsedFoundTables] implicit def decode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#CT]) = AutomaticJsonDecodeBuilder[InContextImpersonatingSaved[MT]]
  }

  case class Standalone[MT <: MetaTypes](soql: String, parameters: Map[HoleName, MT#CT]) extends Query[MT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.Standalone(tree, soql, parameters)
      }
  }
  object Standalone {
    private[UnparsedFoundTables] implicit def encode[MT <: MetaTypes](implicit encCT: JsonEncode[MT#CT]) = AutomaticJsonEncodeBuilder[Standalone[MT]]
    private[UnparsedFoundTables] implicit def decode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#CT]) = AutomaticJsonDecodeBuilder[Standalone[MT]]
  }

  object Query {
    private[analyzer2] implicit def queryEncode[MT <: MetaTypes](implicit encCT : JsonEncode[MT#CT]): JsonEncode[Query[MT]] = SimpleHierarchyEncodeBuilder[Query[MT]](InternalTag("type")).
      branch[Saved[MT]]("saved").
      branch[InContext[MT]]("in-context").
      branch[InContextImpersonatingSaved[MT]]("in-context-impersonating-saved").
      branch[Standalone[MT]]("standalone").
      build

    private[analyzer2] implicit def queryDecode[MT <: MetaTypes](implicit decCT: JsonDecode[MT#CT]): JsonDecode[Query[MT]] = SimpleHierarchyDecodeBuilder[Query[MT]](InternalTag("type")).
      branch[Saved[MT]]("saved").
      branch[InContext[MT]]("in-context").
      branch[InContextImpersonatingSaved[MT]]("in-context-impersonating-saved").
      branch[Standalone[MT]]("standalone").
      build
  }

  implicit def jEncode[MT <: MetaTypes](implicit rnsEncode: JsonEncode[MT#RNS], ctEncode: JsonEncode[MT#CT], dtnEncode: JsonEncode[MT#DatabaseTableNameImpl], dcnEncode: JsonEncode[MT#DatabaseColumnNameImpl]) =
    AutomaticJsonEncodeBuilder[UnparsedFoundTables[MT]]
  implicit def jDecode[MT <: MetaTypes](implicit rnsDecode: JsonDecode[MT#RNS], ctDecode: JsonDecode[MT#CT], dtnDecode: JsonDecode[MT#DatabaseTableNameImpl], dcnDecode: JsonDecode[MT#DatabaseColumnNameImpl]) =
    AutomaticJsonDecodeBuilder[UnparsedFoundTables[MT]]
}

