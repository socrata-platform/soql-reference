package com.socrata.soql.analyzer2

import com.socrata.soql.analyzer2
import com.socrata.soql.functions
import com.socrata.soql.typechecker

// Types of things as seen through the lens of a MetaType
package object types {
  type ColumnType[MT <: MetaTypes] = MT#ColumnType
  type ColumnValue[MT <: MetaTypes] = MT#ColumnValue
  type ResourceNameScope[MT <: MetaTypes] = MT#ResourceNameScope

  type ColumnLabel[MT <: MetaTypes] = analyzer2.ColumnLabel[MT#DatabaseColumnNameImpl]
  type AutoColumnLabel[MT <: MetaTypes] = analyzer2.AutoColumnLabel
  type DatabaseColumnName[MT <: MetaTypes] = analyzer2.DatabaseColumnName[MT#DatabaseColumnNameImpl]

  type TableLabel[MT <: MetaTypes] = analyzer2.TableLabel[MT#DatabaseTableNameImpl]
  type AutoTableLabel[MT <: MetaTypes] = analyzer2.AutoTableLabel
  type DatabaseTableName[MT <: MetaTypes] = analyzer2.DatabaseTableName[MT#DatabaseTableNameImpl]

  type MonomorphicFunction[MT <: MetaTypes] = functions.MonomorphicFunction[MT#ColumnType]

  type TypeInfo[MT <: MetaTypes] = typechecker.TypeInfo[MT#ColumnType, MT#ColumnValue]
  type FunctionInfo[MT <: MetaTypes] = typechecker.FunctionInfo[MT#ColumnType]
  type HasType[MT <: MetaTypes] = typechecker.HasType[MT#ColumnValue, MT#ColumnType]
  type TypeInfoMetaProjection[MT <: MetaTypes] = typechecker.TypeInfoMetaProjection[MT]

  type ScopedResourceName[MT <: MetaTypes] = analyzer2.ScopedResourceName[MT#ResourceNameScope]
  type NameEntry[MT <: MetaTypes] = analyzer2.NameEntry[MT#ColumnType]

  object LabelMap {
    type TableReference[MT <: MetaTypes] = analyzer2.LabelMap.TableReference[MT#ResourceNameScope]
  }

  object TableDescription {
    type DatasetColumnInfo[MT <: MetaTypes] = analyzer2.TableDescription.DatasetColumnInfo[MT#ColumnType]
  }
}
