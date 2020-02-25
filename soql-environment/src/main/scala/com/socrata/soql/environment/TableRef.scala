package com.socrata.soql.environment

sealed abstract class TableRef

object TableRef {
  // An implicit table ref is one that comes from "upstream" (i.e.,
  // it's either the primary table ref or the previous chain step)
  sealed trait Implicit extends TableRef {
    def next: PreviousChainStep
  }

  // A primary candidate can be the primary datset for a select-chain;
  // all PrimaryCandidates are Implicit
  sealed trait PrimaryCandidate extends Implicit

  case object Primary extends PrimaryCandidate {
    override def toString = "primary"
    def next = PreviousChainStep(this, 1)
  }

  case class JoinPrimary(resourceName: ResourceName, joinNumber: Int) extends PrimaryCandidate {
    override def toString = s"join[${resourceName.name}, $joinNumber]"
    def next = PreviousChainStep(this, 1)
  }

  case class PreviousChainStep(root: TableRef with PrimaryCandidate, count: Int) extends Implicit {
    override def toString = s"$root//$count"
    def next = PreviousChainStep(root, count + 1)
    def previous = if(count == 1) root else PreviousChainStep(root, count - 1)
  }

  case class SubselectJoin(root: JoinPrimary) extends TableRef {
    override def toString = s"subselect[$joinNumber]"
    val joinNumber = root.joinNumber
  }

  def serialize(tr: TableRef): String =
    tr match {
      case Primary => PrimaryStr
      case JoinPrimary(resourceName, joinNumber) => JoinPrimaryPfx + Integer.toUnsignedString(joinNumber) + " " + resourceName.name
      case PreviousChainStep(root, count) => PreviousChainStepPfx + Integer.toUnsignedString(count) + " " + serialize(root)
      case SubselectJoin(JoinPrimary(resourceName, joinNumber)) => SubselectJoinPfx + Integer.toUnsignedString(joinNumber) + " " + resourceName.name
    }

  def deserialize(s: String): Option[TableRef] =
    if(s.startsWith(PreviousChainStepPfx)) {
      for {
        (c, rootStr) <- deserializeNumAndString(s.substring(PreviousChainStepPfx.length))
        r <- deserializePrimaryCandidate(rootStr)
      } yield PreviousChainStep(r, c)
    } else if(s.startsWith(SubselectJoinPfx)) {
      deserializeNumAndString(s.substring(SubselectJoinPfx.length)).map { case (c, rn) =>
        SubselectJoin(JoinPrimary(ResourceName(rn), c))
      }
    } else {
      deserializePrimaryCandidate(s)
    }

  private val PrimaryStr = "p"
  private val JoinPrimaryPfx = "jp "
  private val PreviousChainStepPfx = "pc "
  private val SubselectJoinPfx = "sj "

  private def deserializePrimaryCandidate(s: String): Option[PrimaryCandidate] =
    if(s == PrimaryStr) {
      Some(Primary)
    } else if(s.startsWith(JoinPrimaryPfx)) {
      deserializeNumAndString(s.substring(JoinPrimaryPfx.length)).map { case (c, rn) =>
        JoinPrimary(ResourceName(rn), c)
      }
    } else {
      None
    }

  private val NASPattern = """^(\d+) (.*)$""".r

  private def deserializeNumAndString(s: String): Option[(Int, String)] = {
    s match {
      case NASPattern(digits, str) =>
        try {
          Some((Integer.parseUnsignedInt(digits), str))
        } catch {
          case e: NumberFormatException =>
            None
        }
      case _ =>
        None
    }
  }
}
