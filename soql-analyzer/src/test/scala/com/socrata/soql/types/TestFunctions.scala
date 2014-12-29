package com.socrata.soql.types

import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import java.lang.reflect.Modifier
import com.socrata.soql.functions.{FixedType, VariableType, MonomorphicFunction, Function}

sealed abstract class TestFunctions

object TestFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[TestFunctions])

  private val Ordered = TestTypeConversions.typeParameterUniverse.toSet[Any] // might want to narrow this down
  private val NumLike = Set[Any](TestNumber, TestDouble, TestMoney)
  private val AllTypes = SoQLType.typesByName.values.toSet[Any]

  val Concat = Function("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(TestText))
  val Gt = Function(">", SpecialFunctions.Operator(">"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))
  val Lt = Function("<", SpecialFunctions.Operator("<"), Map("a"->Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))

  val Max = Function("max", FunctionName("max"), Map("a"->Ordered), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val Sum = Function("sum", FunctionName("sum"), Map("a"->NumLike), Seq(VariableType("a")), Seq.empty, VariableType("a"), isAggregate = true)
  val CountStar = new MonomorphicFunction("count(*)", SpecialFunctions.StarFunc("count"), Seq(), Seq.empty, TestNumber, isAggregate = true).function

  val NumberToMoney = new MonomorphicFunction("number to money", SpecialFunctions.Operator("to_money"), Seq(TestNumber), Seq.empty, TestMoney).function
  val NumberToDouble = new MonomorphicFunction("number to double", SpecialFunctions.Operator("to_double"), Seq(TestNumber), Seq.empty, TestDouble).function

  val castIdentities = for((n, t) <- TestType.typesByName.toSeq) yield {
    Function(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Map.empty, Seq(FixedType(t)), Seq.empty, FixedType(t))
  }

  val NumberToText = new MonomorphicFunction("number to text", SpecialFunctions.Cast(TestText.name), Seq(TestNumber), Seq.empty, TestText).function
  val TextToNumber = new MonomorphicFunction("text to number", SpecialFunctions.Cast(TestNumber.name), Seq(TestText), Seq.empty, TestNumber).function

  val Prop = new MonomorphicFunction(".", SpecialFunctions.Subscript, Seq(TestObject, TestText), Seq.empty, TestJson).function
  val Index = new MonomorphicFunction("[]", SpecialFunctions.Subscript, Seq(TestArray, TestNumber), Seq.empty, TestJson).function

  val JsonToText = new MonomorphicFunction("json to text", SpecialFunctions.Cast(TestText.name), Seq(TestJson), Seq.empty, TestText).function
  val JsonToNumber = new MonomorphicFunction("json to Number", SpecialFunctions.Cast(TestNumber.name), Seq(TestJson), Seq.empty, TestNumber).function

  val Case = Function("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    Seq(FixedType(SoQLBoolean), VariableType("a")),
    VariableType("a"))

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  for(potentialAccessor <- potentialAccessors) {
    assert(potentialAccessor.getReturnType != classOf[MonomorphicFunction[_]])
  }

  val allFunctions: Seq[Function[TestType]] = {
    val reflectedFunctions = for {
      method <- getClass.getMethods
      if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0 && method.getReturnType == classOf[Function[_]]
    } yield method.invoke(this).asInstanceOf[Function[TestType]]
    castIdentities ++ reflectedFunctions
  }

  val functionsByIdentity = allFunctions.foldLeft(Map.empty[String, Function[TestType]]) { (acc, func) =>
    acc + (func.identity -> func)
  }
  assert(allFunctions.size == functionsByIdentity.size)

  val nAdicFunctions = TestFunctions.allFunctions.filterNot(_.isVariadic)
  val variadicFunctions = TestFunctions.allFunctions.filter(_.isVariadic)

  locally {
    val sharedNames = nAdicFunctions.map(_.name).toSet intersect variadicFunctions.map(_.name).toSet
    assert(sharedNames.isEmpty)
  }

  val nAdicFunctionsByNameThenArity = nAdicFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.minArity).mapValues(_.toSet).toMap
  }.toMap

  val variadicFunctionsByNameThenMinArity = variadicFunctions.groupBy(_.name).mapValues { fs =>
    fs.groupBy(_.minArity).mapValues(_.toSet).toMap
  }.toMap
}
