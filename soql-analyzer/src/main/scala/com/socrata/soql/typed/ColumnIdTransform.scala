package com.socrata.soql.typed

trait ColumnIdTransform[-CID, -QCID, +NewCID, +NewQCID] {
  def mapColumnId(cid: CID): NewCID
  def mapQualifiedColumnId(cid: QCID): NewQCID
}

trait ColumnIdFold[S, -CID, -QCID] {
  def foldColumnId(acc: S, cid: CID): S
  def foldQualifiedColumnId(acc: S, cid: QCID): S
}

trait ColumnIdTransformAccum[S, -CID, -QCID, +NewCID, +NewQCID] {
  def mapColumnId(state: S, cid: CID): (S, NewCID)
  def mapQualifiedColumnId(state: S, cid: QCID): (S, NewQCID)
}
