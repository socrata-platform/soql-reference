package com.socrata.soql.types

import org.joda.time.{LocalDateTime, LocalDate, DateTime, DateTimeZone}

// Postgresql can't store values with the same range as joda-time and
// in particular doesn't like year 0 (which it is within its rights to
// because year 0 doesn't exist).  But given our use-cases, we'll
// tighten it up even more than PG does (PG supports -4713 to
// 294276AD) because years that far out for our customers are almost
// certain to be errors, and it is far easier to loosen bounds here
// than to tighten them.

private object YearInRange {
  def apply(year: Int) = year > 0 && year < 10000
}

class BoundedDateTime private (val dateTime: DateTime) {
  override def hashCode = dateTime.hashCode
  override def equals(o: Any) =
    o match {
      case that: BoundedDateTime => this.dateTime == that.dateTime
      case _ => false
    }
}

object BoundedDateTime {
  def apply(dateTime: DateTime): Option[BoundedDateTime] = {
    if(YearInRange(dateTime.withZone(DateTimeZone.UTC).getYear)) Some(new BoundedDateTime(dateTime))
    else None
  }
}

class BoundedLocalDateTime private (val localDateTime: LocalDateTime) {
  override def hashCode = localDateTime.hashCode
  override def equals(o: Any) =
    o match {
      case that: BoundedLocalDateTime => this.localDateTime == that.localDateTime
      case _ => false
    }
}

object BoundedLocalDateTime {
  def apply(localDateTime: LocalDateTime): Option[BoundedLocalDateTime] = {
    if(YearInRange(localDateTime.getYear)) Some(new BoundedLocalDateTime(localDateTime))
    else None
  }
}


class BoundedLocalDate private (val localDate: LocalDate) {
  override def hashCode = localDate.hashCode
  override def equals(o: Any) =
    o match {
      case that: BoundedLocalDate => this.localDate == that.localDate
      case _ => false
    }
}

object BoundedLocalDate {
  def apply(localDate: LocalDate): Option[BoundedLocalDate] = {
    if(YearInRange(localDate.getYear)) Some(new BoundedLocalDate(localDate))
    else None
  }
}

