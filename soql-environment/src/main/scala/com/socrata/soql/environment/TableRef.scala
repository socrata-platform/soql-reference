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
    override def toString = "primary"
  }

  case class JoinPrimary(resourceName: ResourceName, joinNumber: Int) extends TableRef with PrimaryCandidate {
    override def toString = s"join[${resourceName.name}, $joinNumber]"
  }

  case class PreviousChainStep(root: TableRef with PrimaryCandidate, count: Int) extends TableRef with Implicit {
    override def toString = s"$root//$count"
  }

  case class Join(subselect: Int) extends TableRef {
    override def toString = s"subselect[$subselect]"
  }
}
