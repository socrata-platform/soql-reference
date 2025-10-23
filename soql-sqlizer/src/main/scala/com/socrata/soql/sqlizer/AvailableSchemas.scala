package com.socrata.soql.sqlizer

import scala.collection.compat._

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

case class AvailableSchemas[MT <: MetaTypes with MetaTypesExt](
  instantiated: Map[AutoTableLabel, AugmentedSchema[MT]],
  potential: Map[AutoCTELabel, AugmentedSchema[MT]]
) {
  // "instantiated" schemas are ones that represent the output of a
  // particular query.
  def addInstantiated(labelSchema: (AutoTableLabel, AugmentedSchema[MT])) =
    copy(instantiated = instantiated + labelSchema)

  // "potential" schemas are ones that have not yet been instantiated.
  // For example, the schema of a CTE.
  def addPotential(labelSchema: (AutoCTELabel, AugmentedSchema[MT])) =
    copy(potential = potential + labelSchema)
}

object AvailableSchemas {
  def empty[MT  <: MetaTypes with MetaTypesExt] = AvailableSchemas[MT](Map.empty, Map.empty)
}
