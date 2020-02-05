package com.socrata.soql.environment

sealed abstract class TableRef

object TableRef {
  // An implicit table ref is one that comes from "upstream" (i.e.,
  // it's either the primary table ref or the previous chain step)
  sealed trait Implicit

  case class Primary(resourceName: ResourceName) extends TableRef with Implicit {
    override def toString = resourceName.name
  }

  case object PreviousChainStep extends TableRef with Implicit {
    override def toString = "<previous chain step>"
  }

  case class Join(subselect: Int) extends TableRef {
    override def toString = s"subselect[$subselect]"
  }
}
