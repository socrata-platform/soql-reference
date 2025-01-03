package com.socrata.soql.analyzer2

import java.lang.reflect.Modifier

import com.socrata.soql.collection._
import com.socrata.soql.ast.SpecialFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions._

sealed abstract class TestFunctions

object TestFunctions {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[TestFunctions])

  // TODO: might want to narrow this down
  private val Ordered = CovariantSet.from(TestTypeInfo.typeParameterUniverse.toSet)
  private val Equatable = CovariantSet[TestType](TestText, TestNumber, TestBoolean)
  private val Concatable = CovariantSet[TestType](TestText, TestNumber)
  private val AllTypes = CovariantSet.from(TestType.typesByName.values.toSet)

  // helpers to guide type inference (specifically forces TestType to be inferred)
  private def mf(identity: String, name: FunctionName, params: Seq[TestType], varargs: Seq[TestType], result: TestType, functionType: FunctionType = FunctionType.Normal) =
    new MonomorphicFunction(identity, name, params, varargs, result, functionType)(Function.Doc.empty).function
  private def f(identity: String, name: FunctionName, constraints: Map[String, CovariantSet[TestType]], params: Seq[TypeLike[TestType]], varargs: Seq[TypeLike[TestType]], result: TypeLike[TestType], functionType: FunctionType = FunctionType.Normal) =
    Function(identity, name, constraints, params, varargs, result, functionType, Function.Doc.empty)

  val Case = f("case", FunctionName("case"),
    Map("a" -> AllTypes),
    Seq(FixedType(TestBoolean), VariableType("a")),
    Seq(FixedType(TestBoolean), VariableType("a")),
    VariableType("a")
  )

  val BinaryPlus = f("+", SpecialFunctions.Operator("+"), Map("a" -> Concatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, VariableType("a"))

  val Concat = f("||", SpecialFunctions.Operator("||"), Map.empty, Seq(VariableType("a"), VariableType("b")), Seq.empty, FixedType(TestText))

  val UnaryMinus = mf("-", SpecialFunctions.Operator("-"), Seq(TestNumber), Seq.empty, TestNumber)

  val Times = mf("*", SpecialFunctions.Operator("*"), Seq(TestNumber, TestNumber), Seq.empty, TestNumber)
  val Div = mf("/", SpecialFunctions.Operator("/"), Seq(TestNumber, TestNumber), Seq.empty, TestNumber)

  val And = mf("and", SpecialFunctions.Operator("and"), Seq(TestBoolean, TestBoolean), Seq.empty, TestBoolean)

  val IsNull = f("is null", SpecialFunctions.IsNull, Map(), Seq(VariableType("a")), Seq.empty, FixedType(TestBoolean))

  val Eq = f("=", SpecialFunctions.Operator("="), Map("a" -> Equatable), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))
  val Gt = f(">", SpecialFunctions.Operator(">"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))
  val Lt = f("<", SpecialFunctions.Operator("<"), Map("a" -> Ordered), Seq(VariableType("a"), VariableType("a")), Seq.empty, FixedType(TestBoolean))

  val RowNumber = mf("row_number", FunctionName("row_number"), Nil, Nil, TestNumber, FunctionType.Window(frameAllowed = false))
  val WindowFunction = mf("window_function", FunctionName("window_function"), Nil, Nil, TestNumber, FunctionType.Window(frameAllowed = true))

  val Sum = mf("sum", FunctionName("sum"), Seq(TestNumber), Nil, TestNumber, FunctionType.Aggregate)
  val Avg = mf("avg", FunctionName("avg"), Seq(TestNumber), Nil, TestNumber, FunctionType.Aggregate)
  val Max = mf("max", FunctionName("max"), Seq(TestNumber), Nil, TestNumber, FunctionType.Aggregate)

  val Count = f("count", FunctionName("count"), Map.empty, Seq(VariableType("a")), Nil, FixedType(TestNumber), FunctionType.Aggregate)
  val CountStar = f("count(*)", SpecialFunctions.StarFunc("count"), Map.empty, Nil, Nil, FixedType(TestNumber), FunctionType.Aggregate)
  val CountDistinct = f("count_distinct", FunctionName("count_distinct"), Map.empty, Seq(VariableType("a")), Nil, FixedType(TestNumber), FunctionType.Aggregate)

  // "extract the least signficant 32 bits" and "extract the least
  // significant 8 bits"; these are used for testing the "function
  // subsetting" feature of rollups (in that the latter is a subset of
  // the former)
  val BottomDWord = mf("bottom_dword", FunctionName("bottom_dword"), Seq(TestNumber), Seq.empty, TestNumber)
  val BottomByte = mf("bottom_byte", FunctionName("bottom_byte"), Seq(TestNumber), Seq.empty, TestNumber)
  val BitAnd = mf("bitand", FunctionName("bitand"), Seq(TestNumber, TestNumber), Seq.empty, TestNumber)

  val StringFunc = mf("string_func", FunctionName("string_func"), Seq(TestText), Seq.empty, TestText)

  val castIdentitiesByType = OrderedMap() ++ TestType.typesByName.iterator.map { case (n, t) =>
    t -> mf(n.caseFolded + "::" + n.caseFolded, SpecialFunctions.Cast(n), Seq(t), Seq.empty, t)
  }
  val castIdentities = castIdentitiesByType.valuesIterator.toVector

  def potentialAccessors = for {
    method <- getClass.getMethods
    if Modifier.isPublic(method.getModifiers) && method.getParameterTypes.length == 0
  } yield method

  for (potentialAccessor <- potentialAccessors) {
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
  val windowFunctions = TestFunctions.allFunctions.filter(_.needsWindow)

  locally {
    val sharedNames = nAdicFunctions.map(_.name).toSet intersect variadicFunctions.map(_.name).toSet
    assert(sharedNames.isEmpty)
  }

  val nAdicFunctionsByNameThenArity = nAdicFunctions.groupBy(_.name).view.mapValues { fs =>
    fs.groupBy(_.minArity).view.mapValues(_.toSet).toMap
  }.toMap

  val variadicFunctionsByNameThenMinArity = variadicFunctions.groupBy(_.name).view.mapValues { fs =>
    fs.groupBy(_.minArity).view.mapValues(_.toSet).toMap
  }.toMap
}
