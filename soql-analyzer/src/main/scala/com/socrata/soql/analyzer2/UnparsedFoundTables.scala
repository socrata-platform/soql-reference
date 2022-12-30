package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, SimpleHierarchyEncodeBuilder, SimpleHierarchyDecodeBuilder, InternalTag}

import com.socrata.soql.environment.{ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser

class UnparsedFoundTables[ResourceNameScope, +ColumnType] private[analyzer2] (
  private[analyzer2] val tableMap: UnparsedTableMap[ResourceNameScope, ColumnType],
  private[analyzer2] val initialScope: ResourceNameScope,
  private[analyzer2] val initialQuery: UnparsedFoundTables.Query[ColumnType],
  private[analyzer2] val parserParameters: EncodableParameters
) extends FoundTablesLike[ResourceNameScope, ColumnType] {
  type Self[RNS, +CT] = UnparsedFoundTables[RNS, CT]

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): UnparsedFoundTables[ResourceNameScope, ColumnType] =
    new UnparsedFoundTables(
      tableMap.rewriteDatabaseNames(tableName, columnName),
      initialScope,
      initialQuery,
      parserParameters
    )
}

object UnparsedFoundTables {
  sealed abstract class Query[+CT] {
    private[analyzer2] type SoQL <: String
    private[analyzer2] def soql: SoQL
    private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, FoundTables.Query[CT]]
  }

  case class Saved(name: ResourceName) extends Query[Nothing] {
    private[analyzer2] type SoQL = Nothing
    private[analyzer2] def soql = ???

    private[analyzer2] def parse(params: AbstractParser.Parameters) = Right(FoundTables.Saved(name))
  }
  object Saved {
    private[analyzer2] implicit val codec = AutomaticJsonCodecBuilder[Saved]
  }

  case class InContext[+CT](parent: ResourceName, soql: String, parameters: Map[HoleName, CT]) extends Query[CT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContext(parent, tree, soql, parameters)
      }
  }
  object InContext {
    private[analyzer2] implicit def encode[CT : JsonEncode] = AutomaticJsonEncodeBuilder[InContext[CT]]
    private[analyzer2] implicit def decode[CT : JsonDecode] = AutomaticJsonDecodeBuilder[InContext[CT]]
  }

  case class InContextImpersonatingSaved[+CT](parent: ResourceName, soql: String, parameters: Map[HoleName, CT], fake: CanonicalName) extends Query[CT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContextImpersonatingSaved(parent, tree, soql, parameters, fake)
      }
  }
  object InContextImpersonatingSaved {
    private[UnparsedFoundTables] implicit def encode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[InContextImpersonatingSaved[CT]]
    private[UnparsedFoundTables] implicit def decode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[InContextImpersonatingSaved[CT]]
  }

  case class Standalone[+CT](soql: String, parameters: Map[HoleName, CT]) extends Query[CT] {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil.parseWithoutContext(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.Standalone(tree, soql, parameters)
      }
  }
  object Standalone {
    private[UnparsedFoundTables] implicit def encode[CT: JsonEncode] = AutomaticJsonEncodeBuilder[Standalone[CT]]
    private[UnparsedFoundTables] implicit def decode[CT: JsonDecode] = AutomaticJsonDecodeBuilder[Standalone[CT]]
  }

  object Query {
    private[analyzer2] implicit def queryEncode[CT : JsonEncode]: JsonEncode[Query[CT]] = SimpleHierarchyEncodeBuilder[Query[CT]](InternalTag("type")).
      branch[Saved]("saved").
      branch[InContext[CT]]("in-context").
      branch[InContextImpersonatingSaved[CT]]("in-context-impersonating-saved").
      branch[Standalone[CT]]("standalone").
      build

    private[analyzer2] implicit def queryDecode[CT : JsonDecode]: JsonDecode[Query[CT]] = SimpleHierarchyDecodeBuilder[Query[CT]](InternalTag("type")).
      branch[Saved]("saved").
      branch[InContext[CT]]("in-context").
      branch[InContextImpersonatingSaved[CT]]("in-context-impersonating-saved").
      branch[Standalone[CT]]("standalone").
      build
  }

  implicit def jEncode[RNS: JsonEncode, CT: JsonEncode] =
    AutomaticJsonEncodeBuilder[UnparsedFoundTables[RNS, CT]]
  implicit def jDecode[RNS: JsonDecode, CT: JsonDecode] =
    AutomaticJsonDecodeBuilder[UnparsedFoundTables[RNS, CT]]
}

