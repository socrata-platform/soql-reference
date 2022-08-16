package com.socrata.soql.explore

import scala.concurrent.duration._

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.simplearm.v2._
import com.socrata.http.server._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.{StrongEntityTag, Precondition}
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.analyzer2.{SoQLAnalyzer, UserParameters, Annotation, Statement}
import com.socrata.soql.types._
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctionInfo, SoQLFunctions}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.typechecker.HasDoc

object Main extends App {
  private def escapeHtml(s: String) = {
    s.codePoints().mapToObj { c =>
      c match {
        case '<' => "&lt;"
        case '>' => "&gt;"
        case '"' => "&quot;"
        case '\'' => "&apos;"
        case '&' => "&amp;"
        case other => new String(Character.toChars(other))
      }
    }.collect(java.util.stream.Collectors.joining)
  }

  def debugHtml[RNS, CT, CV](thing: Statement[RNS, CT, CV], width: Int)(implicit ev1: HasDoc[CV], ev2: JsonEncode[RNS], ev3: JsonEncode[CT]): String =
    Renderer.renderHtml[RNS, CT](thing.debugDoc.layoutSmart(LayoutOptions(pageWidth = PageWidth.AvailablePerLine(width, 1.0))).asTree)

  implicit val hasDoc = new HasDoc[SoQLValue] {
    val cryptProvider = new CryptProvider(CryptProvider.generateKey())

    def docOf(v: SoQLValue) = v.doc(cryptProvider)
  }

  class Static(contentType: String, path: String) extends SimpleResource {
    private val content = using(getClass.getResourceAsStream(path)) { rs =>
      val baos = new ByteArrayOutputStream
      val buf = new Array[Byte](4096)
      def loop() {
        rs.read(buf) match {
          case -1 => // done
          case n => baos.write(buf, 0, n); loop()
        }
      }
      loop()
      baos.toByteArray
    }

    val cachebust =
      MessageDigest.getInstance("SHA-1").
        digest(content).
        map { i => "%02x".format(i & 0xff) }.mkString

    override def get: HttpRequest => HttpResponse = { req =>
      OK ~>
        Header("Cache-control", "public, max-age=604800, immutable") ~>
        ContentType(contentType) ~>
        ContentBytes(content)
    }
  }


  def result(
    scope: MockTableFinder[String, SoQLType],
    query: String,
    html: String,
    merge: Boolean,
    preserveOrder: Boolean,
    useSelectRefs: Boolean
  ): String = {
    s"""<!DOCTYPE html>
<html>
<head>
  <script src="soqlexplore.js?${js.cachebust}"></script>
  <link rel="stylesheet" href="soqlexplore.css?${css.cachebust}">
</head>
<body>
<form id="form" method="post">
  <textarea name="scope">${escapeHtml(JsonUtil.renderJson(scope, pretty = true))}</textarea>
  <textarea name="query">${escapeHtml(query)}</textarea>
  <input type="submit">
  <input type="checkbox" id="merge" name="merge" value="1"${if(merge)" checked"else""}><label for="merge">Merge</label>
  <input type="checkbox" id="preserve_order" name="preserve_order" value="1"${if(preserveOrder)" checked"else""}><label for="preserve_order">Preserve order</label>
  <input type="checkbox" id="select_refs" name="select_refs" value="1"${if(useSelectRefs)" checked"else""}><label for="select_refs">Use selection references</label>
</form>
<div class="soql" id="rendered">$html</div>
<div>
<h1>Instructions</h1>
<p>The left textbox defines the datasets, views, and UDFs available to the analyzer.  The top-level keys are the scopes in which the defined objects live - these multiple scopes demonstrate something like federation or other access control.  The names defined here are the names of things in the "database" (or rather, names that will be mapped directly to the names of things in the database).</p>
<p>The right textbox defines the query you want to typecheck.  Any tables referenced in this query will be looked up in scope <tt>"one"</tt>.</p>
<p>Having created your schemas and query, clicking "submit query" will send them to the server for validation.  Right now error handling is.. bad.  Sorry.  It's a demo I worked up on my weekend. :)<p>
<p>If you get a good answer though, you will find a somewhat-colored analysis returned, which will look roughly like SQL.  Mousing over the various colored bits will show cross-references (other references to a thing are highlighted in yellow, the place where a thing is defined is highlighted in green.  Expressions will pop up a tooltip showing the type of the expression under the mouse, and the aliases for both tables and select-lists will show the human names that correlate to the generated labels.
<p>In addition, you can control a set of postprocessing passes over the analysis:</p>
<dl>
<dt>Merge<dd>Attempt to merge queries with their FROM clause.
<dt>Preserve order<dd>Add additional columns and ORDER BY clauses to preserve the ordering of subselects, if appropriate.
<dt>Use selection references<dd>Rewrite expressions into numeric selection-list references where appropriate (inside <tt>GROUP&nbsp;BY</tt>, <tt>ORDER&nbsp;BY</tt>, and <tt>DISTINCT&nbsp;ON</tt> clauses, for nontrivial expressions).
</dl>
</div>
</body>
</html>"""
  }

  val defaultTableFinder = MockTableFinder[String, SoQLType](
    ("one", "twocol") -> D(
      ":id" -> SoQLID,
      ":version" -> SoQLVersion,
      "text" -> SoQLText,
      "num" -> SoQLNumber
    ),
    ("one", "evens") -> Q("one", "twocol", "select :*, * where num % 2 == 0"),
    ("one", "odds") -> Q("one", "twocol", "select :*, * where num % 2 == 1"),
    ("two", "users") -> D(
      ":id" -> SoQLID,
      ":version" -> SoQLVersion,
      "name" -> SoQLText,
      "is_admin" -> SoQLBoolean
    ),
    ("two", "secret_data") -> D(
      ":id" -> SoQLID,
      ":version" -> SoQLVersion,
      "key" -> SoQLText,
      "value" -> SoQLText,
      "row_requires_admin" -> SoQLBoolean
    ),
    ("two", "permcheck") -> U("two", "select 1 from @users where name = get_context('name') and (is_admin or not ?need_to_be_admin)", "need_to_be_admin" -> SoQLBoolean),
    ("one", "secured_view") -> Q("two", "secret_data", "select :*, * (except row_requires_admin) join @permcheck(row_requires_admin) on true")
  )

  val analyzer = new SoQLAnalyzer[String, SoQLType, SoQLValue](SoQLTypeInfo, SoQLFunctionInfo)

  val index = new SimpleResource {
    override def get: HttpRequest => HttpResponse = { req =>
      OK ~> Header("Cache-Control", "no-cache") ~> Content("text/html; charset=UTF-8", result(defaultTableFinder, "select * from @evens\n  join @secured-view on text = @secured-view.key\n  where num > 3", "", true, true, true))
    }

    override def post: HttpRequest => HttpResponse = {
      def process(req: HttpRequest): HttpResponse = {
        val params = req.servletRequest.getParameterMap
        val width = Option(params.get("width")) match {
          case Some(wstr) =>
            try {
              wstr(0).toInt
            } catch {
              case _ : NumberFormatException =>
                return BadRequest ~> Content("text/plain", "bad width?")
            }
          case None =>
            return BadRequest ~> Content("text/plain", "No width?")
        }

        val scopes = Option(params.get("scope")).getOrElse {
          return BadRequest ~> Content("text/plain", "No scope?")
        }
        val tableFinder = try {
          JsonUtil.parseJson[MockTableFinder[String, SoQLType]](scopes(0)) match {
            case Right(s) => s
            case Left(e) =>
              return BadRequest ~> Content("text/plain", "Malformed scope json:\n" + e.english)
          }
        } catch {
          case e: JsonReaderException =>
            e.printStackTrace
            return BadRequest ~> Content("text/plain", "Malformed scope json:\n" + e.getMessage)
        }
        val query = Option(params.get("query")).getOrElse {
          return BadRequest ~> Content("text/plain", "No query?")
        }

        val merge = Option(params.get("merge")).isDefined
        val preserveOrder = Option(params.get("preserve_order")).isDefined
        val useSelectRefs = Option(params.get("select_refs")).isDefined

        val map = tableFinder.findTables("one", query(0)) match {
          case tableFinder.Success(map) => map
          case tableFinder.Error.ParseError(name, error) =>
            val msg = name match {
              case Some((scope, name)) => s"Parse error in saved query ${scope}/${name}: $error"
              case None => s"Parse error: $error"
            }
            return BadRequest ~> Content("text/plain", msg)
          case tableFinder.Error.NotFound((scope, name)) =>
            return BadRequest ~> Content("text/plain", s"Not found: $scope/$name")
          case tableFinder.Error.PermissionDenied(name) =>
            return InternalServerError ~> Content("text/plain", "God a permission error somehow?")
        }

        var analysis =
          try {
            analyzer(map, UserParameters.emptyFor(map))
          } catch {
            case e: Throwable =>
              return BadRequest ~> Write("text/plain") { (w: java.io.Writer) =>
                val pw = new java.io.PrintWriter(w)
                e.printStackTrace(pw)
                pw.flush()
              }
          }

        if(merge) analysis = analysis.merge(SoQLFunctions.And.monomorphic.get)
        if(preserveOrder) analysis = analysis.preserveOrdering(SoQLFunctions.RowNumber.monomorphic.get)
        if(useSelectRefs) analysis = analysis.useSelectListReferences

        val html = debugHtml(analysis.statement.useSelectListReferences, width)

        OK ~> Content("text/html; charset=UTF-8", result(tableFinder, query(0), html, merge, preserveOrder, useSelectRefs))
      }

      val srv: HttpRequest => HttpResponse = process(_)
      srv.andThen(Header("Cache-Control", "no-cache") ~> _)
    }
  }

  val css = new Static("text/css; charset=UTF-8", "soqlexplore.css")
  val js = new Static("text/javascript; charset=UTF-8", "soqlexplore.js")

  val handler = new HttpService {
    val routingTable = Routes(
      Route("/", index),
      Route("/soqlexplore.css", css),
      Route("/soqlexplore.js", js)
    )

    def apply(req: HttpRequest): HttpResponse =
      routingTable(req.requestPath) match {
        case Some(resource) => resource(req)
        case None => NotFound
      }
  }

  val server = new SocrataServerJetty(handler, SocrataServerJetty.defaultOptions.withGracefulShutdownTimeout(1.millisecond).withDeregisterWait(1.millisecond))
  server.run()
}
