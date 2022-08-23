package com.socrata.metanalyze2

import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.util.{JsonUtil, JsonKey, AlternativeJsonKey, AutomaticJsonCodecBuilder}
import com.rojoma.simplearm.v2._
import com.rojoma.sql.v1.{JdbcConnectionProvider, Query, AutomaticSqlTypeBuilder}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection._
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctionInfo, SoQLFunctions}
import com.socrata.soql.typechecker.HasDoc

final abstract class Main

object Main extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  def toSoQLType(name: String) = name match {
    case "text" => SoQLText
    case "number" => SoQLNumber
    case "date" => SoQLFixedTimestamp
    case "phone" => SoQLPhone
    case "checkbox" => SoQLBoolean
    case "url" => SoQLUrl
    case "photo" => SoQLPhoto
    case "document" => SoQLDocument
    case "calendar_date" => SoQLFloatingTimestamp
    case "point" => SoQLPoint
    case "multipoint" => SoQLMultiPoint
    case "line" => SoQLLine
    case "multiline" => SoQLMultiLine
    case "polygon" => SoQLPolygon
    case "multipolygon" => SoQLMultiPolygon
    case other => throw new Exception("Unknown soql type: " + other)
  }

  val config = JsonUtil.readJsonFile[Config]("config.json", StandardCharsets.UTF_8) match {
    case Right(c) => c
    case Left(e) => throw new Exception(e.english)
  }

  using(new ResourceScope) { rs =>
    val connProvider = rs.open(new JdbcConnectionProvider(
      "jdbc:postgresql://" + config.database.hostport + "/" + config.database.database,
      config.database.username,
      config.database.password
    ))

    val conn = connProvider.openImmediate(rs)

    case class LensId(id: Int)
    object LensId { implicit val ast = AutomaticSqlTypeBuilder[LensId] }
    case class BlistId(id: Int)
    object BlistId { implicit val ast = AutomaticSqlTypeBuilder[BlistId] }
    case class LensColumnId(id: Int)
    object LensColumnId { implicit val ast = AutomaticSqlTypeBuilder[LensColumnId] }
    case class DomainId(id: Int)
    object DomainId { implicit val ast = AutomaticSqlTypeBuilder[DomainId] }

    case class Lens(
      id: LensId,
      uid: String,
      domainId: DomainId,
      blistId: BlistId,
      modifyingLensId: Option[LensId],
      queryString: Option[String],
      isDefault: Boolean,
      viewType: String,
      udfDefinition: Option[String],
      privateMetadata: Option[String],
      clientContext: Option[String]
    )

    val domainQ = Query[String, DomainId]("select id from (select ? as s) as x join domains on true where cname = x.s or aliases like '%,' || x.s || ',%'")
    val domainP = conn.prepare(domainQ, rs)

    case class LensSpace(domainId: DomainId, name: String)

    val lensQ = Query[LensSpace, Lens]("""
select id, uid, domain_id, blist_id, modifying_lens_id, query_string, is_default, view_type, udf_definition, private_metadata, client_context
from lenses
where
  (
    domain_id = @domainId
    or domain_id in (
      select source_domain_id
      from data_federations
      where
        target_domain_id = @domainId
        and coalesce(lens_id, lenses.id) = lenses.id
        and accepted_user_id is not null
        and deleted_at is null
    )
  )
  and (uid = @name or resource_name = @name)
  and deleted_at is null
""")
    val lensP = conn.prepare(lensQ, rs)
    val defaultLensQ = Query[BlistId, Lens]("""
select id, uid, domain_id, blist_id, modifying_lens_id, query_string, is_default, view_type, udf_definition, private_metadata, client_context
from lenses
where
  blist_id = ?
  and is_default
  and deleted_at is null
""")
    val defaultLensP = conn.prepare(defaultLensQ, rs)
    val lensByIdQ = Query[LensId, Lens]("""
select id, uid, domain_id, blist_id, modifying_lens_id, query_string, is_default, view_type, udf_definition, private_metadata, client_context
from lenses
where
  id = ?
  and deleted_at is null
""")
    val lensByIdP = conn.prepare(lensByIdQ, rs)

    case class LensColumn(id: LensColumnId, fieldName: String, dbName: String)
    val lensColumnQ = Query[LensId, LensColumn]("""
select lc.id, lc.field_name, dt.db_name
from lens_columns lc
  join blist_columns bc on lc.blist_column_id = bc.id
  join data_types dt on bc.data_type_id = dt.id
where
  lc.lens_id = ?
  and lc.deleted_at is null
""")
    val lensColumnP = conn.prepare(lensColumnQ, rs)

    case class UDFParameter(name: String, @JsonKey("type") @AlternativeJsonKey("typ") typ: String)
    object UDFParameter { implicit val jCodec = AutomaticJsonCodecBuilder[UDFParameter] }

    case class UDFDefinition(soql: String, parameters: Seq[UDFParameter]) {
      val parametersMap = OrderedMap() ++ parameters.iterator.map { param => HoleName(param.name) -> toSoQLType(param.typ) }

      def withOverrides(uid: String, overrides: Overrides): UDFDefinition = {
        overrides.parameters.get(uid) match {
          case None => this
          case Some(typeOverrides) =>
            val replacement = copy(
              parameters = parameters.map { param =>
                typeOverrides.get(param.name) match {
                  case None => param
                  case Some(t) => param.copy(typ = t)
                }
              }
            )
            log.info("Patching parameter types: old (${parameters.map(_.typ).mkString(",")}); new (${replacement.parameters.map(_.typ).mkString(",")})")
            replacement
        }
      }
    }
    object UDFDefinition { implicit val jCodec = AutomaticJsonCodecBuilder[UDFDefinition] }

    case class ClientContextVariable(name: String, dataType: String)
    object ClientContextVariable { implicit val jCodec = AutomaticJsonCodecBuilder[ClientContextVariable] }
    case class ClientContext(clientContextVariables: Seq[ClientContextVariable]) {
      val parametersMap = clientContextVariables.iterator.map { param => HoleName(param.name) -> toSoQLType(param.dataType.toLowerCase) }.toMap
    }
    object ClientContext { implicit val jCodec = AutomaticJsonCodecBuilder[ClientContext] }

    val tableFinder = new TableFinder {
      type ColumnType = SoQLType
      type ParseError = Nothing
      type ResourceNameScope = DomainId

      def parse(soql: String, udfParamsAllowed: Boolean) =
        Right(
          new StandaloneParser(AbstractParser.defaultParameters.copy(allowHoles = udfParamsAllowed)).
            binaryTreeSelect(soql)
        )

      def patchSoql(uid: String, soql: String): String =
        config.overrides.soqlPatch.get(uid) match {
          case None =>
            soql
          case Some(replacements) =>
            val replacement = replacements.foldLeft(soql) { (soql, fromto) =>
              soql.replaceFirst(java.util.regex.Pattern.quote(fromto._1), java.util.regex.Matcher.quoteReplacement(fromto._2))
            }
            log.info(s"Patching soql: old length ${soql.length}; new length ${replacement.length}")
            replacement
        }

      def lookup(scope: DomainId, name: ResourceName): Either[LookupError, TableDescription] = {
        log.info(s"Looking up $scope : $name")
        lensP.query(LensSpace(scope, name.name)).run(_.nextOption()) match {
          case Some(lens) =>
            log.info(s"Found ${lens.uid}")
            val columns = lensColumnP.query(lens.id).run { results =>
              results.toVector
            }
            if(lens.viewType == "parameterized_view") {
              val udfDefJson = lens.udfDefinition.orElse(lens.privateMetadata).getOrElse {
                throw new Exception(lens.uid + " : parameteterized_view without udf definition")
              }
              val udfDefinition = JsonUtil.parseJson[UDFDefinition](udfDefJson) match {
                case Left(e) => throw new Exception(lens.uid + " : " + e.english)
                case Right(d) => d.withOverrides(lens.uid, config.overrides)
              }
              for((c, t) <- udfDefinition.parametersMap) {
                log.debug(s"  ${c} : ${t}")
              }
              Right(TableFunction(
                scope = lens.domainId,
                canonicalName = CanonicalName(lens.uid),
                soql = patchSoql(lens.uid, udfDefinition.soql),
                parameters = udfDefinition.parametersMap
              ))
            } else if(lens.isDefault) {
              val schema = columns.map { c =>
                ColumnName(c.fieldName) -> toSoQLType(c.dbName)
              }
              for((c, t) <- schema) {
                log.debug(s"  ${c} : ${t}")
              }
              Right(Dataset(
                DatabaseTableName(lens.uid),
                OrderedMap(
                  ColumnName(":id") -> SoQLID,
                  ColumnName(":version") -> SoQLVersion,
                  ColumnName(":created_at") -> SoQLFixedTimestamp,
                  ColumnName(":updated_at") -> SoQLFixedTimestamp
                ) ++ schema
              ))
            } else {
              val queryString = lens.queryString.getOrElse {
                throw new Exception(lens.uid + " : non-default lens without query_string")
              }
              val basedOn =
                (lens.modifyingLensId match {
                   case None => defaultLensP.query(lens.blistId).run(_.nextOption())
                   case Some(mlid) => lensByIdP.query(mlid).run(_.nextOption())
                 }).getOrElse {
                  throw new Exception(lens.uid + " : couldn't find parent lens")
                }
              val clientContext =
                lens.clientContext.map { cc =>
                  JsonUtil.parseJson[ClientContext](cc) match {
                    case Right(cc) => cc
                    case Left(err) => throw new Exception(lens.uid + " : invalid client context : " + err.english)
                  }
                }
              Right(Query(
                scope = lens.domainId,
                canonicalName = CanonicalName(lens.uid),
                basedOn = ResourceName(basedOn.uid),
                soql = patchSoql(lens.uid, queryString),
                parameters = clientContext.map(_.parametersMap).getOrElse(Map.empty)
              ))
            }
          case None =>
            Left(LookupError.NotFound)
        }
      }
    }

    implicit val soqlDoc = new HasDoc[SoQLValue] {
      val cp = new obfuscation.CryptProvider(new Array[Byte](72))
      def docOf(v: SoQLValue) = v.doc(cp)
    }

    val Array(domainName, views @ _*) = args
    val domainId = domainP.query(domainName).run(_.nextOption()) match {
      case Some(did) => did
      case None => throw new Exception("No such domain : " + domainName)
    }
    log.info("{}", domainId)
    for(uid <- views) {
      tableFinder.findTables(domainId, ResourceName(uid)) match {
        case tableFinder.Success(tm) =>
          val analysis = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)(tm, UserParameters.emptyFor(tm))
          log.info(analysis.removeUnusedColumns.merge(SoQLFunctions.And.monomorphic.get).preserveOrdering(SoQLFunctions.RowNumber.monomorphic.get).useSelectListReferences.statement.debugStr)
        case other =>
          log.info("{}", other)
      }
    }
  }
}
