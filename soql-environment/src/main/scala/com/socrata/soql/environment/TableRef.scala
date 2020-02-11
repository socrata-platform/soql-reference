package com.socrata.soql.environment

sealed abstract class TableRef

object TableRef {
  // An implicit table ref is one that comes from "upstream" (i.e.,
  // it's either the primary table ref or the previous chain step)
  sealed trait Implicit

  // A primary candidate can be the primary datset for a select-chain;
  // all PrimaryCandidates are Implicit
  sealed trait PrimaryCandidate extends Implicit

  case object Primary extends TableRef with PrimaryCandidate {
    override def toString = "<primary>"
  }

  case class JoinPrimary(resourceName: ResourceName) extends TableRef with PrimaryCandidate {
    override def toString = resourceName.name
  }

  case object PreviousChainStep extends TableRef with Implicit {
    override def toString = "<previous chain step>"
  }

  case class Join(subselect: Int) extends TableRef {
    override def toString = s"subselect[$subselect]"
  }
}
