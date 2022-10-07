package com.socrata.soql.analyzer2

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, AutomaticJsonDecodeBuilder, AutomaticJsonCodecBuilder, SimpleHierarchyCodecBuilder, InternalTag}

import com.socrata.soql.environment.ResourceName
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser

class UnparsedFoundTables[ResourceNameScope, +ColumnType] private[analyzer2] (
  private[analyzer2] val tableMap: UnparsedTableMap[ResourceNameScope, ColumnType],
  private[analyzer2] val initialScope: ResourceNameScope,
  private[analyzer2] val initialQuery: UnparsedFoundTables.Query,
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
  sealed abstract class Query {
    private[analyzer2] type SoQL <: String
    private[analyzer2] def soql: SoQL
    private[analyzer2] def parse(params: AbstractParser.Parameters): Either[LexerParserException, FoundTables.Query]
  }

  case class Saved(name: ResourceName) extends Query {
    private[analyzer2] type SoQL = Nothing
    private[analyzer2] def soql = ???

    private[analyzer2] def parse(params: AbstractParser.Parameters) = Right(FoundTables.Saved(name))
  }
  object Saved {
    private[analyzer2] implicit val codec = AutomaticJsonCodecBuilder[Saved]
  }

  case class InContext(parent: ResourceName, soql: String) extends Query {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContext(parent, tree, soql)
      }
  }
  object InContext {
    private[analyzer2] implicit val codec = AutomaticJsonCodecBuilder[InContext]
  }

  case class InContextImpersonatingSaved(parent: ResourceName, soql: String, fake: CanonicalName) extends Query {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.InContextImpersonatingSaved(parent, tree, soql, fake)
      }
  }
  object InContextImpersonatingSaved {
    private[UnparsedFoundTables] implicit val codec = AutomaticJsonCodecBuilder[InContextImpersonatingSaved]
  }

  case class Standalone(soql: String) extends Query {
    private[analyzer2] type SoQL = String
    private[analyzer2] def parse(params: AbstractParser.Parameters) =
      ParserUtil(soql, params.copy(allowHoles = false)).map { tree =>
        FoundTables.Standalone(tree, soql)
      }
  }
  object Standalone {
    private[UnparsedFoundTables] implicit val codec = AutomaticJsonCodecBuilder[Standalone]
  }

  object Query {
    private[analyzer2] implicit val queryCodec: JsonEncode[Query] with JsonDecode[Query] = SimpleHierarchyCodecBuilder[Query](InternalTag("type")).
      branch[Saved]("saved").
      branch[InContext]("in-context").
      branch[InContextImpersonatingSaved]("in-context-impersonating-saved").
      branch[Standalone]("standalone").
      build
  }

  implicit def jEncode[RNS: JsonEncode, CT: JsonEncode] =
    AutomaticJsonEncodeBuilder[UnparsedFoundTables[RNS, CT]]
  implicit def jDecode[RNS: JsonDecode, CT: JsonDecode] =
    AutomaticJsonDecodeBuilder[UnparsedFoundTables[RNS, CT]]
}

