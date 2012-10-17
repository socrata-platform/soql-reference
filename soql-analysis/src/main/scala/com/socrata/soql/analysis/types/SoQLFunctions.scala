package com.socrata.soql.analysis.types

import com.socrata.soql.names.FunctionName
import com.socrata.soql.analysis.{FixedType, VariableType, MonomorphicFunction, Function}
import java.lang.reflect.Modifier
import com.socrata.soql.ast.SpecialFunctions

object SoQLFunctions {
  val TextToFixedTimestamp = new MonomorphicFunction(FunctionName("to_fixed_timestamp"), Seq(SoQLText), SoQLFixedTimestamp).function
  val TextToFloatingTimestamp = new MonomorphicFunction(FunctionName("to_floating_timestamp"), Seq(SoQLText), SoQLFloatingTimestamp).function
  val Concat = Function(SpecialFunctions.Operator("||"), Seq(VariableType("a"), VariableType("b")), FixedType(SoQLText))
  val Gt = Function(SpecialFunctions.Operator(">"), Seq(VariableType("a"), VariableType("a")), FixedType(SoQLBoolean))
  val LatitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("latitude")), SoQLDouble).function
  val LongitudeField = new MonomorphicFunction(SpecialFunctions.Subscript, Seq(SoQLLocation, SoQLTextLiteral("longitude")), SoQLDouble).function

  lazy val allFunctions = {
    for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[SoQLType]]
  }

  lazy val functionsByNameThenArity = SoQLFunctions.allFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.arity).mapValues(_.toSet).toMap
  }.toMap
}
