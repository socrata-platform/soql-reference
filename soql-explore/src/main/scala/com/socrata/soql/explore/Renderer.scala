package com.socrata.soql.explore

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.{SimpleDocTree, tree}

import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.analyzer2.{Annotation, ColumnLabel, AutoColumnLabel, TableLabel, AutoTableLabel, DatabaseColumnName, DatabaseTableName}
import com.socrata.soql.analyzer2.Annotation._

object Renderer {
  def renderHtml[RNS: JsonEncode, CT: JsonEncode](doc: SimpleDocTree[Annotation[RNS, CT]]): String = {
    val renderer = new Renderer[RNS, CT]
    renderer.go(tree.Ann(TableDefinition(DatabaseTableName(java.util.UUID.randomUUID().toString)), doc))
    renderer.result
  }

  private class Renderer[RNS: JsonEncode, CT: JsonEncode] {
    private val sb = new StringBuilder

    def result = sb.toString

    private implicit object rnRenderer extends JsonEncode[ResourceName] {
      def encode(rn: ResourceName) = JString(rn.name)
    }
    private implicit object cnRenderer extends JsonEncode[ColumnName] {
      def encode(rn: ColumnName) = JString(rn.name)
    }

    def go(node: SimpleDocTree[Annotation[RNS, CT]]): Unit = {
      node match {
        case tree.Char(c) =>
          sb.append(escapeHtml(c.toString))
        case tree.Text(text) =>
          sb.append(escapeHtml(text))
        case tree.Line(indent) =>
          sb.append("<br>")
          for(_ <- 0 until indent) {
            sb.append("&nbsp;");
          }
        case tree.Concat(elems) =>
          elems.foreach(go)
        case tree.Ann(TableAliasDefinition(None, table), doc) =>
          sb.append("<span class=\"table-alias-def\" data-table=\"").append(clsOf(table)).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(TableAliasDefinition(Some(name), table), doc) =>
          sb.append("<span class=\"table-alias-def\" data-table=\"").append(clsOf(table)).append("\" data-username=\"").append(escapeAttr(JsonUtil.renderJson(name))).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(ColumnAliasDefinition(name, label), doc) =>
          sb.append("<span class=\"column-alias-def\" data-name=\"").append(escapeAttr(JsonUtil.renderJson(name))).append("\" data-label=\"").append(clsOf(label)).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(TableDefinition(label), doc) =>
          sb.append("<span class=\"table-def\" data-label=\"").append(clsOf(label)).append("\" style=\"--table-label: ").append(clsOf(label)).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(SelectListDefinition(idx), doc) =>
          sb.append("<span class=\"select-list-def\" data-idx=\"").append(idx).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(SelectListReference(idx), doc) =>
          sb.append("<span class=\"select-list-ref\" data-idx=\"").append(idx).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(Typed(typ), doc) =>
          sb.append("<span class=\"typed\" data-type=\"").append(escapeAttr(JsonUtil.renderJson(typ))).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Ann(ColumnRef(tbl, col), doc) =>
          sb.append("<span class=\"column-ref\" data-table=\"").append(clsOf(tbl)).append("\" data-column=\"").append(clsOf(col)).append("\">")
          go(doc)
          sb.append("</span>")
        case tree.Empty =>
          // do nothing
      }
    }

    private def escapeHtml(s: String) = {
      s.codePoints().mapToObj { c =>
        c match {
          case '<' => "&lt;"
          case '>' => "&gt;"
          case ' ' => "&nbsp;"
          case '"' => "&quot;"
          case '\'' => "&apos;"
          case '&' => "&amp;"
          case other => new String(Character.toChars(other))
        }
      }.collect(java.util.stream.Collectors.joining)
    }
    private def escapeAttr(s: String) = escapeHtml(s)
    private def clsOf(label: TableLabel) = "tbl_" + hash(label)
    private def clsOf(label: ColumnLabel) = "col_" + hash(label)

    def hash(l: TableLabel) = l match {
      case DatabaseTableName(tn) => sha1(0, tn)
      case a: AutoTableLabel => sha1(1, a.toString)
    }

    def hash(l: ColumnLabel) = l match {
      case DatabaseColumnName(cn) => sha1(0, cn)
      case a: AutoColumnLabel => sha1(1, a.toString)
    }

    private def sha1(b: Byte, s: String) = {
      val md = java.security.MessageDigest.getInstance("SHA1")
      md.update(b)
      md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)).iterator.map("%02x".format(_)).mkString
    }
  }
}
