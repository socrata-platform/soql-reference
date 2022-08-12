package com.socrata.soql.analyzer2.mocktablefinder

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.Select
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName}
import com.socrata.soql.parsing.standalone_exceptions.LexerParserException
import com.socrata.soql.parsing.{StandaloneParser, AbstractParser}
import com.socrata.soql.analyzer2.{TableFinder, DatabaseTableName, ParsedTableDescription, CanonicalName}

sealed abstract class Thing[+RNS, +CT]
case class D[+CT](schema: Map[String, CT]) extends Thing[Nothing, CT]
case class Q[+RNS, +CT](scope: RNS, parent: String, soql: String, params: (String, CT)*) extends Thing[RNS, CT]
case class U[+RNS, CT](scope: RNS, soql: String, params: (String, CT)*) extends Thing[RNS, CT]

object MockTableFinder {
  def empty[RNS, CT] = new MockTableFinder[RNS, CT](Map.empty)
  def apply[RNS, CT](items: ((RNS, String), Thing[RNS, CT])*) = new MockTableFinder[RNS, CT](items.toMap)
}

class MockTableFinder[RNS, CT](raw: Map[(RNS, String), Thing[RNS, CT]]) extends TableFinder {
  private val tables = raw.iterator.map { case ((scope, rawResourceName), thing) =>
    val converted = thing match {
      case D(rawSchema) =>
        Dataset(
          DatabaseTableName(rawResourceName),
          OrderedMap() ++ rawSchema.iterator.map {case (rawColumnName, ct) =>
            ColumnName(rawColumnName) -> ct
          }
        )
      case Q(scope, parent, soql, params @ _*) =>
        Query(scope, CanonicalName(rawResourceName), ResourceName(parent), soql, params.iterator.map { case (k, v) => HoleName(k) -> v }.toMap)
      case U(scope, soql, params @ _*) =>
        TableFunction(scope, CanonicalName(rawResourceName), soql, OrderedMap() ++ params.iterator.map { case (k,v) => HoleName(k) -> v })
    }
      (scope, ResourceName(rawResourceName)) -> converted
  }.toMap

  type ResourceNameScope = RNS
  type ParseError = LexerParserException
  type ColumnType = CT

  protected def lookup(scope: RNS, name: ResourceName): Either[LookupError, TableDescription] = {
    tables.get((scope, name)) match {
      case Some(schema) =>
        Right(schema)
      case None =>
        Left(LookupError.NotFound)
    }
  }

  protected def parse(soql: String, udfParamsAllowed: Boolean): Either[ParseError, BinaryTree[Select]] = {
    try {
      Right(
        new StandaloneParser(AbstractParser.defaultParameters.copy(allowHoles = udfParamsAllowed)).
          binaryTreeSelect(soql)
      )
    } catch {
      case e: ParseError =>
        Left(e)
    }
  }

  private def parsed(thing: TableDescription) = {
    thing match {
      case ds: Dataset => ds.toParsed
      case Query(scope, canonicalName, parent, soql, params) => ParsedTableDescription.Query(scope, canonicalName, parent, parse(soql, false).getOrElse(throw new Exception("broken soql fixture 1")), params)
      case TableFunction(scope, canonicalName, soql, params) => ParsedTableDescription.TableFunction(scope, canonicalName, parse(soql, false).getOrElse(throw new Exception("broken soql fixture 2")), params)
    }
  }

  def apply(names: (RNS, String)*): Success[TableMap] = {
    val r = names.map { case (scope, n) =>
      val name = ResourceName(n)
      (scope, name) -> parsed(tables((scope, name)))
    }.toMap

    if(r.size != names.length) {
      throw new Exception("Malformed table list")
    }

    Success(new TableMap(r))
  }

  def notFound(scope: RNS, name: String) =
    Error.NotFound((scope, ResourceName(name)))
}
