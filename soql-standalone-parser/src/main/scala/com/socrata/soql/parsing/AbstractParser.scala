package com.socrata.soql.parsing

import com.socrata.soql.collection.NonEmptySeq

import com.socrata.soql.BinaryTree
import com.socrata.soql.ast.{Expression, Join, JoinSelect, OrderBy, Select, Selection}
import com.socrata.soql.environment.ColumnName

object AbstractParser {
  case class Parameters(allowJoins: Boolean = true,
                        systemColumnAliasesAllowed: Set[ColumnName] = Set.empty,
                        allowJoinFunctions: Boolean = true,
                        allowHoles: Boolean = false,
                        allowInSubselect: Boolean = false)
  val defaultParameters = new Parameters()
}

trait AbstractParser {
  def binaryTreeSelect(soql: String): BinaryTree[Select]
  def selection(soql: String): Selection
  def joins(soql: String): Seq[Join]
  def expression(soql: String): Expression
  def orderings(soql: String): Seq[OrderBy]
  def groupBys(soql: String): Seq[Expression]
  def selectStatement(soql: String): NonEmptySeq[Select]
  def unchainedSelectStatement(soql: String): Select
  def parseJoinSelect(soql: String): JoinSelect
  def limit(soql: String): BigInt
  def offset(soql: String): BigInt
  def search(soql: String): String
}
