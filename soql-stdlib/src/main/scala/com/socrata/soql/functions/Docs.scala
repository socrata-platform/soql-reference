package com.socrata.soql.functions

import com.socrata.soql.types._

object Docs {
    def main(args: Array[String]) {
        val out_path = args(0)
        SoQLFunctions.allFunctions.filter(x => x.name.toString != "op$[]").foreach { function => 
            val doc = makeFuncDoc(function)
            println(function.name.toString)
            val cleanedName = cleanName(function.name.toString)
            val file_path = out_path + s"$cleanedName.md"
            println(file_path)
            val f = new java.io.File(file_path)
            if(!f.exists) f.createNewFile
            val fw = new java.io.FileWriter(f)
            try fw.write(doc) finally fw.close
        }
    }
    def cleanName(name: String): String = {
        val rep = name.replace("op$", "").replace("/*", "-*").replace("$", "_")
        val operators = Map(
            "+" -> "add",
            "-" -> "subtract",
            "*" -> "multiply",
            "/" -> "divide",
            "%" -> "modulo",
            "^" -> "exponent",
            "||" -> "concatenate",
            ">" -> "greater_than",
            "<" -> "less_than",
            ">=" -> "greater_than_equal",
            "<=" -> "less_than_equal" ,
            "==" -> "equal",
            "=" -> "equal",
            "!=" -> "not_equal",
            "<>" -> "not_equal",
            "or" -> "or",
            "and" -> "and",
            "not" -> "not"
        )
        operators.getOrElse(rep, rep)
    }
    def makeFuncDoc(function: Function[SoQLType]): String = {
s"""---
layout: with-sidebar
sidebar: documentation
title: "${cleanTitle(function.name.toString)}"

type: function
function: ${
    if(function.name.toString.contains("op")) "$1" + s"${cleanTitle(function.name.toString)}" + "$2"
    else s"${cleanTitle(function.name.toString)}(${(1 to function.parameters.length).map{ i => "$" + s"${i}" }.mkString(", ")})"
}
description: "${function.doc}" 
versions:
- 2.1
datatypes:
${
function.parameters.map {
      case FixedType(typ) => s"- $typ"
      case VariableType(name) => function.constraints.getOrElse(name, Set.empty[SoQLType]).map { t => s"- $t"}.mkString("\n")
}.toSet.mkString("\n")
}
parent_paths: 
- /docs/functions/
parents: 
- SoQL Function Listing 
---
{% include function_header.html %}

## Description
${function.doc}

${function.examples.map { exp => 
    "## Explanation\n" + exp.explanation + "\n" +
    "## Query\n" + exp.query + "\n" +
    "## Try it\n" + exp.tryit
}.mkString("\n")}

## Signature
${function.toString}
""".stripMargin
    }
    def cleanTitle(name: String): String = {
        name.replace("op$", "")
    }
}
