package com.socrata.soql.analyzer2

import scala.language.higherKinds

import com.rojoma.json.v3.ast.{JValue, JObject, JString}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonDecodeBuilder}

import com.socrata.soql.ast
import com.socrata.soql.environment.{ResourceName, HoleName, ColumnName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.AbstractParser
import com.socrata.soql.BinaryTree

trait FoundTablesLike[ResourceNameScope, +ColumnType] {
  type Self[RNS, +CT]

  def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): Self[ResourceNameScope, ColumnType]
}

case class UserParameterSpecs[+ColumnType](
  qualified: Map[CanonicalName, Map[HoleName, ColumnType]],
  unqualified: Either[CanonicalName, Map[HoleName, ColumnType]]
)

final case class FoundTables[ResourceNameScope, +ColumnType] private[analyzer2] (
  tableMap: TableMap[ResourceNameScope, ColumnType],
  initialScope: ResourceNameScope,
  initialQuery: FoundTables.Query[ColumnType],
  parserParameters: AbstractParser.Parameters
) extends FoundTablesLike[ResourceNameScope, ColumnType] {
  type Self[RNS, +CT] = FoundTables[RNS, CT]

  def asUnparsedFoundTables =
    new UnparsedFoundTables(
      tableMap.asUnparsedTableMap,
      initialScope,
      initialQuery.asUnparsedQuery,
      EncodableParameters.fromParams(parserParameters)
    )

  val knownUserParameters: UserParameterSpecs[ColumnType] = {
    val named =
      tableMap.descriptions.foldLeft(Map.empty[CanonicalName, Map[HoleName, ColumnType]]) { (acc, desc) =>
        desc match {
          case _ : TableDescription.Dataset[_] | _ : TableDescription.TableFunction[_, _] => acc
          case q: TableDescription.Query[_, ColumnType] => acc + (q.canonicalName -> q.parameters)
        }
      }
    initialQuery match {
      case FoundTables.Saved(_) => UserParameterSpecs(named, Right(Map.empty))
      case FoundTables.InContext(_, _, _, params) => UserParameterSpecs(named, Right(params))
      case FoundTables.InContextImpersonatingSaved(_, _, _, params, fake) =>
        assert(!named.contains(fake)) // the stack while finding tables should prevent this from ever happening
        UserParameterSpecs(named + (fake -> params), Left(fake))
      case FoundTables.Standalone(_, _, params) => UserParameterSpecs(named, Right(params))
    }
  }

  final def rewriteDatabaseNames(
    tableName: DatabaseTableName => DatabaseTableName,
    // This is given the _original_ database table name
    columnName: (DatabaseTableName, DatabaseColumnName) => DatabaseColumnName
  ): FoundTables[ResourceNameScope, ColumnType] =
    copy(tableMap = tableMap.rewriteDatabaseNames(tableName, columnName))

  // This lets you convert resource scope names to a simplified form
  // if your resource scope names in one location have semantic
  // meaning that you don't care to serialize.  You also get a map
  // from the meaningless name to the meaningful one so if you want to
  // (for example) translate an error from the analyzer back into the
  // meaningful form, you can do that.
  lazy val (withSimplifiedScopes, simplifiedScopeMap) = locally {
    val (newMap, newToOld, oldToNew) = tableMap.rewriteScopes(initialScope)

    val newFT = FoundTables(
      newMap,
      oldToNew(initialScope),
      initialQuery,
      parserParameters
    )

    (newFT, newToOld)
  }
}

object FoundTables {
  sealed abstract class Query[+CT] {
    def asUnparsedQuery: UnparsedFoundTables.Query[CT]
  }

  case class Saved(name: ResourceName) extends Query[Nothing] {
    def asUnparsedQuery = UnparsedFoundTables.Saved(name)
  }

  case class InContext[+CT](parent: ResourceName, soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, CT]) extends Query[CT] {
    def asUnparsedQuery = UnparsedFoundTables.InContext(parent, text, parameters)
  }
  case class InContextImpersonatingSaved[+CT](parent: ResourceName, soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, CT], fake: CanonicalName) extends Query[CT] {
    def asUnparsedQuery = UnparsedFoundTables.InContextImpersonatingSaved(parent, text, parameters, fake)
  }
  case class Standalone[+CT](soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, CT]) extends Query[CT] {
    def asUnparsedQuery = UnparsedFoundTables.Standalone(text, parameters)
  }

  private implicit def queryJsonEncode[CT : JsonEncode] = new JsonEncode[Query[CT]] {
    def encode(v: Query[CT]) = JsonEncode.toJValue(v.asUnparsedQuery)
  }
  private def queryJsonDecode[CT : JsonDecode](params: AbstractParser.Parameters): JsonDecode[Query[CT]] =
    new JsonDecode[Query[CT]] {
      def decode(x: JValue) =
        JsonDecode[UnparsedFoundTables.Query[CT]].decode(x).flatMap { c =>
          c.parse(params.copy(allowHoles = false)).left.map { _ =>
            DecodeError.InvalidValue(JString(c.soql)).prefix("soql")
          }
        }
    }

  implicit def jsonEncode[RNS: JsonEncode, CT: JsonEncode]: JsonEncode[FoundTables[RNS, CT]] =
    new JsonEncode[FoundTables[RNS, CT]] {
      def encode(v: FoundTables[RNS, CT]) = JsonEncode.toJValue(v.asUnparsedFoundTables)
    }

  implicit def jsonDecode[RNS: JsonDecode, CT: JsonDecode]: JsonDecode[FoundTables[RNS, CT]] =
    new JsonDecode[FoundTables[RNS, CT]] {
      def decode(x: JValue): Either[DecodeError, FoundTables[RNS, CT]] = x match {
        case JObject(fields) =>
          val params =
            fields.get("parserParameters") match {
              case Some(params) =>
                EncodableParameters.paramsCodec.decode(params) match {
                  case Right(p) => p.toParameters
                  case Left(e) => return Left(e.prefix("parserParameters"))
                }
              case None =>
                return Left(DecodeError.MissingField("parserParameters"))
            }

          implicit val tableMapDecode = TableMap.jsonDecode[RNS, CT](params)
          implicit val queryDecode = queryJsonDecode[CT](params)

          case class DecodeHelper(
            tableMap: TableMap[RNS, CT],
            initialScope: RNS,
            initialQuery: FoundTables.Query[CT]
          )

          AutomaticJsonDecodeBuilder[DecodeHelper].decode(x).map { dh =>
            new FoundTables(dh.tableMap, dh.initialScope, dh.initialQuery, params)
          }
        case other =>
          Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
    }
}

