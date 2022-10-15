package com.socrata.soql.analyzer2

// I'm not sure this is actually useful, but in a world where we don't
// care about RNS anymore (e.g., SQL generation) we can import things
// from dealiased and not need to bother with setting RNS everywhere.
package object dealiased {
  import com.socrata.soql.{analyzer2 => a2}

  type Statement[+CT, +CV] = a2.Statement[Any, CT, CV]
  type CombinedTables[+CT, +CV] = a2.CombinedTables[Any, CT, CV]
  type CTE[+CT, +CV] = a2.CTE[Any, CT, CV]
  type Values[+CT, +CV] = a2.Values[CT, CV]
  type Select[+CT, +CV] = a2.Select[Any, CT, CV]

  type From[+CT, +CV] = a2.From[Any, CT, CV]
  type Join[+CT, +CV] = a2.Join[Any, CT, CV]
  type AtomicFrom[+CT, +CV] = a2.AtomicFrom[Any, CT, CV]
  type FromTable[+CT] = a2.FromTable[Any, CT]
  type FromStatement[+CT, +CV] = a2.FromStatement[Any, CT, CV]
  type FromSingleRow = a2.FromSingleRow[Any]

  type SoQLAnalysis[CT, CV] = a2.SoQLAnalysis[Any, CT, CV]
  type SoQLAnalyzer[CT, CV] = a2.SoQLAnalyzer[Any, CT, CV]
}
