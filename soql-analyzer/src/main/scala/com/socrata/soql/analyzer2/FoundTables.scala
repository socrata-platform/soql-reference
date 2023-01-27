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
import com.socrata.soql.analyzer2

trait FoundTablesLike[MT <: MetaTypes] extends MetaTypeHelper[MT] with LabelHelper[MT] {
  type Self[MT <: MetaTypes] <: FoundTablesLike[MT]

  def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
    columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
  )(implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): Self[MT2]
}

case class UserParameterSpecs[+ColumnType](
  qualified: Map[CanonicalName, Map[HoleName, ColumnType]],
  unqualified: Either[CanonicalName, Map[HoleName, ColumnType]]
)

final case class FoundTables[MT <: MetaTypes] private[analyzer2] (
  tableMap: TableMap[MT],
  initialScope: MT#ResourceNameScope,
  initialQuery: FoundTables.Query[MT],
  parserParameters: AbstractParser.Parameters
) extends FoundTablesLike[MT] {
  type Self[MT <: MetaTypes] = FoundTables[MT]

  def asUnparsedFoundTables =
    new UnparsedFoundTables[MT](
      tableMap.asUnparsedTableMap,
      initialScope,
      initialQuery.asUnparsedQuery,
      EncodableParameters.fromParams(parserParameters)
    )

  val knownUserParameters: UserParameterSpecs[CT] = {
    val named =
      tableMap.descriptions.foldLeft(Map.empty[CanonicalName, Map[HoleName, CT]]) { (acc, desc) =>
        desc match {
          case _ : TableDescription.Dataset[_] | _ : TableDescription.TableFunction[_] => acc
          case q: TableDescription.Query[_] => acc + (q.canonicalName -> q.parameters)
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

  final def rewriteDatabaseNames[MT2 <: MetaTypes](
    tableName: DatabaseTableName => analyzer2.DatabaseTableName[MT2#DatabaseTableNameImpl],
    columnName: (DatabaseTableName, DatabaseColumnName) => analyzer2.DatabaseColumnName[MT2#DatabaseColumnNameImpl]
  )(implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): FoundTables[MT2] =
    copy(
      tableMap = tableMap.rewriteDatabaseNames[MT2](tableName, columnName),
      initialScope = changesOnlyLabels.convertRNS(initialScope),
      initialQuery = initialQuery.changeLabels[MT2]
    )

  // This lets you convert resource scope names to a simplified form
  // if your resource scope names in one location have semantic
  // meaning that you don't care to serialize.  You also get a map
  // from the meaningless name to the meaningful one so if you want to
  // (for example) translate an error from the analyzer back into the
  // meaningful form, you can do that.
  lazy val (withSimplifiedScopes, simplifiedScopeMap) = locally {
    val (newMap, newToOld, oldToNew) = tableMap.rewriteScopes(initialScope)

    val newFT = FoundTables[Intified[MT]](
      newMap,
      oldToNew(initialScope),
      initialQuery.changeRNS[Intified[MT]],
      parserParameters
    )

    (newFT, newToOld)
  }
}

final class Intified[MT <: MetaTypes] extends MetaTypes {
  type ResourceNameScope = Int
  type ColumnType = MT#ColumnType
  type ColumnValue = MT#ColumnValue
  type DatabaseTableNameImpl = MT#DatabaseTableNameImpl
  type DatabaseColumnNameImpl = MT#DatabaseColumnNameImpl
}

object FoundTables {
  sealed abstract class Query[MT <: MetaTypes] {
    def asUnparsedQuery: UnparsedFoundTables.Query[MT]
    def changeRNS[MT2 <: MetaTypes](implicit changesOnlyRNS: ChangesOnlyRNS[MT, MT2]): Query[MT2]
    def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): Query[MT2]
  }

  case class Saved[MT <: MetaTypes](name: ResourceName) extends Query[MT] {
    def asUnparsedQuery = UnparsedFoundTables.Saved(name)
    def changeRNS[MT2 <: MetaTypes](implicit changesOnlyRNS: ChangesOnlyRNS[MT, MT2]): Saved[MT2] =
      this.asInstanceOf[Saved[MT2]] // SAFETY: we don't care about _anything_ in MT
    def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): Saved[MT2] =
      this.asInstanceOf[Saved[MT2]] // SAFETY: we don't care about _anything_ in MT
  }

  case class InContext[MT <: MetaTypes](parent: ResourceName, soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, MT#CT]) extends Query[MT] {
    def asUnparsedQuery = UnparsedFoundTables.InContext(parent, text, parameters)
    def changeRNS[MT2 <: MetaTypes](implicit changesOnlyRNS: ChangesOnlyRNS[MT, MT2]): InContext[MT2] =
      this.asInstanceOf[InContext[MT2]] // SAFETY: we only care about CT, which isn't changing
    def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): InContext[MT2] =
      this.asInstanceOf[InContext[MT2]] // SAFETY: we only care about CT, which isn't changing
  }
  case class InContextImpersonatingSaved[MT <: MetaTypes](parent: ResourceName, soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, MT#CT], fake: CanonicalName) extends Query[MT] {
    def asUnparsedQuery = UnparsedFoundTables.InContextImpersonatingSaved(parent, text, parameters, fake)
    def changeRNS[MT2 <: MetaTypes](implicit changesOnlyRNS: ChangesOnlyRNS[MT, MT2]): InContextImpersonatingSaved[MT2] =
      this.asInstanceOf[InContextImpersonatingSaved[MT2]] // SAFETY: we only care about CT, which isn't changing
    def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): InContextImpersonatingSaved[MT2] =
      this.asInstanceOf[InContextImpersonatingSaved[MT2]] // SAFETY: we only care about CT, which isn't changing
  }
  case class Standalone[MT <: MetaTypes](soql: BinaryTree[ast.Select], text: String, parameters: Map[HoleName, MT#CT]) extends Query[MT] {
    def asUnparsedQuery = UnparsedFoundTables.Standalone(text, parameters)
    def changeRNS[MT2 <: MetaTypes](implicit changesOnlyRNS: ChangesOnlyRNS[MT, MT2]): Standalone[MT2] =
      this.asInstanceOf[Standalone[MT2]] // SAFETY: we only care about CT, which isn't changing
    def changeLabels[MT2 <: MetaTypes](implicit changesOnlyLabels: ChangesOnlyLabels[MT, MT2]): Standalone[MT2] =
      this.asInstanceOf[Standalone[MT2]] // SAFETY: we only care about CT, which isn't changing
  }

  private implicit def queryJsonEncode[MT <: MetaTypes](implicit encCT : JsonEncode[MT#CT]) = new JsonEncode[Query[MT]] {
    def encode(v: Query[MT]) = JsonEncode.toJValue(v.asUnparsedQuery)
  }
  private def queryJsonDecode[MT <: MetaTypes](params: AbstractParser.Parameters)(implicit decCT : JsonDecode[MT#CT]): JsonDecode[Query[MT]] =
    new JsonDecode[Query[MT]] {
      def decode(x: JValue) =
        JsonDecode[UnparsedFoundTables.Query[MT]].decode(x).flatMap { c =>
          c.parse(params.copy(allowHoles = false)).left.map { _ =>
            DecodeError.InvalidValue(JString(c.soql)).prefix("soql")
          }
        }
    }

  implicit def jsonEncode[MT <: MetaTypes](implicit rnsEncode: JsonEncode[MT#RNS], ctEncode: JsonEncode[MT#CT], dtnEncode: JsonEncode[MT#DatabaseTableNameImpl], dcnEncode: JsonEncode[MT#DatabaseColumnNameImpl]): JsonEncode[FoundTables[MT]] =
    new JsonEncode[FoundTables[MT]] {
      def encode(v: FoundTables[MT]) = JsonEncode.toJValue(v.asUnparsedFoundTables)
    }

  implicit def jsonDecode[MT <: MetaTypes](implicit rnsDecode: JsonDecode[MT#RNS], ctDecode: JsonDecode[MT#CT], dtnDecode: JsonDecode[MT#DatabaseTableNameImpl], dcnDecode: JsonDecode[MT#DatabaseColumnNameImpl]) =
    new JsonDecode[FoundTables[MT]] with MetaTypeHelper[MT] {
      def decode(x: JValue): Either[DecodeError, FoundTables[MT]] = x match {
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

          implicit val tableMapDecode = TableMap.jsonDecode[MT](params)
          implicit val queryDecode = queryJsonDecode[MT](params)

          case class DecodeHelper(
            tableMap: TableMap[MT],
            initialScope: RNS,
            initialQuery: FoundTables.Query[MT]
          )

          AutomaticJsonDecodeBuilder[DecodeHelper].decode(x).map { dh =>
            new FoundTables(dh.tableMap, dh.initialScope, dh.initialQuery, params)
          }
        case other =>
          Left(DecodeError.InvalidType(expected = JObject, got = other.jsonType))
      }
    }
}

