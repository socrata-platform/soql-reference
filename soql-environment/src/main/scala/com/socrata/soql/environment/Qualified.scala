package com.socrata.soql.environment

case class Qualified[C](table: TableRef, columnName: C)
