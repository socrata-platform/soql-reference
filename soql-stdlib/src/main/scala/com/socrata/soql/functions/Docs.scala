package com.socrata.soql.functions

import com.socrata.soql.types._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.rojoma.simplearm.v2._
import com.rojoma.json.v3.ast.JString

object Docs {
    def main(args: Array[String]) {
        val out_path = args(0)
        SoQLFunctions.allFunctions.filter(x => x.name match { 
            case SpecialFunctions.Subscript => false
            case _ => true
        }).groupBy(_.name).foreach { case(name, functions) => 
            val doc = makeFuncDoc(name, functions)
            println(name.toString)
            val cleanedName = cleanName(name.toString)
            val file_path = out_path + s"$cleanedName.md"
            println(file_path)
            for {
                fos <- managed(new java.io.FileOutputStream(file_path))
                osw <- managed(new java.io.OutputStreamWriter(fos, java.nio.charset.StandardCharsets.UTF_8))
            } {
                osw.write(doc)
            }
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
    def makeFuncDoc(name: FunctionName, functions: Seq[Function[SoQLType]]): String = {
s"""---
layout: with-sidebar
sidebar: documentation
title: ${JString(cleanTitle(name.toString))}

type: function
function: ${
    if(name.toString.contains("op")) "$1" + s" ${cleanTitle(name.toString)} " + "$2"
    else s"${cleanTitle(name.toString)}(${(1 to functions.head.parameters.length).map{ i => "$" + s"${i}" }.mkString(", ")})"
}
description: ${
    // It doesn't matter which one we use if there are multiple overloads
    if(functions.head.doc.length > 55) JString(functions.head.doc.slice(0, 55)+"...")
    else JString(functions.head.doc)
}
versions:
- 2.1
datatypes:
${functions.map{ function => function.parameters.headOption match {
        case Some(FixedType(typ)) => s"- ${JString(typ.toString)}"
        case Some(VariableType(name)) => function.constraints.getOrElse(name, SoQLTypeClasses.Ordered ++ SoQLTypeClasses.GeospatialLike).map { t => s"- ${JString(t.toString)}"}.mkString("\n")
        case None => ""
    }
}.toSet.mkString("\n")
}
returns: 
${functions.head.result match {
    case FixedType(typ) => s"- ${JString(typ.toString)}"
    case VariableType(name) => functions.head.constraints.getOrElse(name, SoQLTypeClasses.Ordered ++ SoQLTypeClasses.GeospatialLike).map { t => s"- ${JString(t.toString)}"}.mkString("\n")
    }
}
parent_paths: 
- /docs/functions/
parents: 
- SoQL Function Listing 
---
{% include function_header.html %}

## Description
${functions.head.doc}

${functions.head.examples.map { exp => 
    "## Explanation\n" + exp.explanation + "\n" +
    "## Query\n" + exp.query + "\n" +
    "## Try it\n" + exp.tryit
}.mkString("\n")}

## Signature
${functions.map(f => s"${JString(f.identity)} => " + f.toString.replace("op$", "")).mkString("\n\n")}
""".stripMargin
    }
    def cleanTitle(name: String): String = {
        name.replace("op$", "")
    }
}
