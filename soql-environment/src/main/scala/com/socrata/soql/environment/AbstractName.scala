package com.socrata.soql.environment

import com.ibm.icu.text.{Normalizer2, Normalizer}

abstract class AbstractName[Self <: AbstractName[Self]](val name: String) extends Ordered[Self] {
  final lazy val caseFolded = AbstractName.caseFold(name)

  protected def hashCodeSeed: Int
  override lazy val hashCode = caseFolded.hashCode ^ hashCodeSeed

  override final def equals(o: Any) =
    if(getClass.isInstance(o))
      this.caseFolded.equals(o.asInstanceOf[AbstractName[_]].caseFolded)
    else false

  final def compare(that: Self) =
    this.caseFolded.compareTo(that.caseFolded)

  override final def toString = name
}

object AbstractName {
  private val nfdNormalizer = Normalizer2.getNFDInstance
  private def nfd(s: String) = nfdNormalizer.normalize(s)

  private val nfkcCasefolder = Normalizer2.getNFKCCasefoldInstance
  private def nfkcCasefold(s: String) = nfkcCasefolder.normalize(s)

  private def foldDashes(s: String) = {
    val cs = new Array[Char](s.length)
    var i = 0
    while(i != cs.length) {
      val c = s.charAt(i)
      if(c == '-') cs(i) = '_'
      else cs(i) = c
      i += 1
    }
    new String(cs)
  }

  // Folding for Unicode identifier caseless matching plus dash/underscore equivalence
  private def caseFold(s: String) = nfkcCasefold(foldDashes(nfd(s)))
}
