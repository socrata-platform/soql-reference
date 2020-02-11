package com.socrata.soql.environment

sealed abstract class TableRef

object TableRef {
  // An implicit table ref is one that comes from "upstream" (i.e.,
  // it's either the primary table ref or the previous chain step)
  sealed trait Implicit {
    def next: PreviousChainStep
  }

  // A primary candidate can be the primary datset for a select-chain;
  // all PrimaryCandidates are Implicit
  sealed trait PrimaryCandidate extends Implicit

  case object Primary extends TableRef with PrimaryCandidate {
    override def toString = "primary"
    def next = PreviousChainStep(this, 1)
  }

  case class JoinPrimary(resourceName: ResourceName, joinNumber: Int) extends TableRef with PrimaryCandidate {
    override def toString = s"join[${resourceName.name}, $joinNumber]"
    def next = PreviousChainStep(this, 1)
  }

  case class PreviousChainStep(root: TableRef with PrimaryCandidate, count: Int) extends TableRef with Implicit {
    override def toString = s"$root//$count"
    def next = PreviousChainStep(root, count + 1)
  }

  case class Join(subselect: Int) extends TableRef {
    override def toString = s"subselect[$subselect]"
  }
}
