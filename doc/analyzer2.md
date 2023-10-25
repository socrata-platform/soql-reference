The Wonderful World of Analyzer2

# Naming things

Things in the analyzer2 world have multiple names, depending on what
level you're working on.  For example, dataset columns have both the
field name that appears in soql queries and a database name whose
value depends on what database the dataset appears in.  Similarly,
tables have a uid and possibly a resource name and a database name.

Intermediate tables and columns also have names.  While some of these
have source-level names, all of them have table or column labels which
are assigned at analysis-time.  These are called `AutoTableLabel` and
`AutoColumnLabel` respectively; the combination of such a pair of
labels uniquely identifies a virtual column within the soql query
(note that an `AutoColumnLabel` _alone_ does not!).  So for example,
given the SoQL query
```
select x, y from @some-table
  |> select x * 2, y, @s2.x from @some-table as @s2 on y = @s2.y
```
the analyzer will give labels something like this:
```
(select t1.x as c1, t1.y as c2 from @some-table as t1) as t2
  |> select t2.c1 * 2 as c3, t2.c2 as c4, t3.x as c5 from @some-table as t3 on t2.c2 = t3.y
```
Note that `t1.x` and `t3.x` refer to different "instantiations" (for
lack of a better word) of `@some-table`.  In a real analysis, the
original user-provided names are preserved alongside these labels.

# MetaTypes

Many of the classes in analyzer2 are very generic.  In order to keep
this noise down to a minimum, the common generic parameters are
bundled up into a single `MetaTypes` trait, which just contains the
other types.

For example,

```scala
trait MyMetaTypes extends MetaTypes {
    type ResourceNameScope = DomainId
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = (RealTableName, PublicationStage)
    type DatabaseColumnNameImpl = ColumnId
}

// with metatypes
val analyzer = new SoQLAnalyzer[MyMetaTypes](...)

// without metatypes this would have to be something like
val analyzer = new SoQLAnalyzer[DomainId, SoQLType, SoQLValue, (RealTableName, PublicationStage), ColumnId](...)
```

## Label helpers

There are a collection of helper mixin traits defined alongside
`MetaTypes` that help reduce boilerplate when using them.  The vast
majority of the time, code is written in a single (possibly abstract)
`MetaTypes` scope and explicitly mentioning the metatypes' trait all
the time increases noise.

### MetaTypeHelper

Mixing in `MetaTypeHelper` brings aliases in for the most-used
metatypes; specifically it aliases `MetaTypes#ColumnType` to `CT`,
`MetaTypes#ColumnValue` to `CV`, and `MetaTypes#ResourceNameScope` to
`RNS`.

```scala
class Something[MT <: MetaTypes] { // no mixin
    def takesAColumnType(typ: MT#ColumnType)
}

class Something[MT <: MetaTypes] extends MetaTypeHelper[MT] {
    def takesAColumnType(typ: CT)
}
```

### LabelUniverse

`LabelUniverse` extends `MetaTypesHelper` with aliases for the various
kinds of "labels", so you get all the names defined in
`MetaTypesHelper` but you also get aliases for things like
`DatabaseTableName` (rather than needing to refer to
`DatabaseTableName[MT#DatabaseTableNameImpl]`), `DatabaseColumnName`,
`ColumnLabel`, `ScopedResourceName`, and the like.

### ExpressionUniverse

This extends `LabelUniverse` with aliases for `Expr` and all its
subclasses, along with `OrderBy` and `MonomorphicFunction`, which also
can appear in expression-handling code.

### StatementUniverse

Finally `StatementUniverse` extends `ExpressionUniverse` with support
for all types you encounder when processing whole `Statements`.

# TableFinder

A `TableFinder` is the library's interface to a catalog of tables,
saved views, and UDFs.  It takes a user's query and finds all the
things referenced in it, bundling them up into a single `FoundTables`
object.

The `TableFinder` is an abstract class with a single method that needs
to be implemented, `lookup` which takes a scoped resource name and
returns a value (a `Dataset`, `Query`, or `TableFunction`) which
represents the desired object.  For `Dataset`s and `Query`s, it is
possible to annotate columns with _hints_ and _hiddenness_.  A hint is
a json value (opaque to this codebase) which is passed through
analysis and query execution and back out to the calling code.  If an
output column has no hint and it is a simple reference to a column on
one of the tables it it built from, it inherits any hint from that
parent column.  A hidden column is invisible to any query which
consumes the output of the current query.

Table names in the table finder are _scoped_ which allows the same
resource name to refer to different objects, and for the objects to
control what scope other objects that they themselves reference are
stored in.  For example, in Core, a scope is approximately a (user,
domain) pair.  The user is used to do permission checking, and the
domain is used to control what domain resource names (as opposed to
uids) are considered in.

`FoundTables` are JSON-serializable (if all the relevant metatypes are
JSON-serializable) and so can be sent around across services.  They
are also intended to be a stable format for long-term storage of saved
queries, for example in rollups.

There's a helper class called `MockTableFinder` mainly intended for
use with tests which allows you to easily create a universe of things
to be looked up.

# SoQLAnalyzer

The soql analyzer does semantic analysis on the result of finding
tables and produces a result which is easy to convert to other query
formats (most specifically SQL).  It has a few responsibilities:

* Typecheck the code
* Verify that aggregate and window functions are being used correctly.
* Propagate system columns through, if requested
* Convert table and column names to labels which can be used to more
  easily identify the referant of a column reference.

# SoQLAnalysis

A `SoQLAnalysis` is the result of a successful analysis operation.  It
contains a single `Statement` and a collection of methods for
performing various post-processing passes.  These passes are defined
in the `rewrite` subpackage of analyzer2, and can perform various
query simplification passes (e.g., combining stacked selects together
or removing unused columns) or utility operations that are useful in a
user interface (e.g., requesting a particular page of results).

# Converting between different `MetaTypes`

Frequently during processing you'll want to convert from one kind of
MetaTypes to another.  Notably, the representations of
DatabaseTableNames and DatabaseColumnNames will vary as a query
descends through the stack.  There are helper methods on most classes
for doing this kind of conversion.

`FoundTables` contains a helper method to convert arbitrary resource
name scopes to `Int` (and then back again, if desired).  This is used
as once table-finding is complete, the details of the scope don't
matter as much, so each distinct scope is converted to a distinct
integer and any internal details of the scope are lost unless it's
looked up again going the other way.

# Statements

The core of the output of an analysis is a single `Statement` object
whose structure more or less closely matches that of SQL.

Statements' code structure is slightly unusual, done so that the
various subclasses' implementations can live in separate files while
simultaneously allowing the Statement hierarchy as a whole to be
sealed.  Each concrete subclass inherits both from `Statement[MT]` as
well as an implementation trait which is mixed in.

All `Statement`s produce a table result of some kind, and there are a
number of ways to introspect them without matching to a more specific
subtype:
* `schema` returns the schema of the output table
* `unique` returns a stream of lists of column labels, where each item
  in the stream is a collection of columns which uniquely identify
  rows.
* `allTables` returns the database names of all tables transitively
  referenced by this statement.
* `isIsomorphic` and `isomorphicTo` test whether two `Statement`s
  represent the "same query", which means if they have the same
  structure including an isomorphic mapping between the auto-generated
  column and table labels.  The difference between them is that
  `isIsomorphic` simply returns a boolean, while `isomorphicTo`
  returns that label mapping if it exists.
* `isVerticalSlice` and `verticalSliceOf` are similar, except instead
  of determining whether two queries are exactly the same, they
  determine if the left statement returns some subset of the right
  statement's columns.

The last operation of note on `Statement` is `rewriteDatabaseNames`
which is used to convert a `Statement` from one `MetaTypes` to
another, where the two `MetaTypes` are the same save for the table and
column names.

There are a handful of other `Statement`-level methods but they aren't
super useful in practice.

## CombinedTables

A `CombinedTables` is a statement which is a binary operation on
sub-selections, e.g., `union` and the like.  It consists of an
operation and two sub-statements, and has the requirement (checked
dynamically in the constructor) that both sub-statements' output
schemas have the same shape.

## CTE

This is an incomplete sketch of a SQL common table expression.  It is
probably not in its final form, and there is presently no way to
generate one from SoQL source.

## Values

`Values` statements represent anonymous literal tables.  There is
likewise presently no way to create such a statement in SoQL
(formerly, table-function desugaring would produce a single-row
`Values` but this was changed in order to allow a `Statement` to be
converted back into an AST for debugging purposes).

## Select

`Select` is the main subclass of `Statement`.  It is structurally
pretty close to the structure of a source SoQL query - the main
difference is that where SoQL does not have a required `FROM`
subclause and has a _separate_ `JOIN` subclause, in a `Select` these
are combined into a single (more SQL-like) `from` field.

# From

`From` represents the source table which a `Select` transforms.  This
may be a simple database table, the output of another query, or a join
of multiple tables.

`From` uses the same sealed-split system as `Statement`.

There are two kinds of `From`s, `Join` which represents multiple
tables being combined via a cartesian product, and `AtomicFrom` which
is every other kind of source table.  A `Join` can be thought of as an
annotated, left-associated "cons cell" for arranging `AtomicFrom`s
into a list.

Like `Statement`, `From` has `unique` and `schema` methods with
similar semantics but slightly different return types, as well as the
isomorphism and vertical slice functions.  In addition, it has methods
for processing joins, including the "trivial join" of a single table:
* `reduceMap` is the primitive operation for processing - it's a
  combined map and fold which takes two functions - the first one
  receives the leftmost `AtomicFrom` in the join chain and it returns
  a state value and a new `AtomicFrom` (possibly in a different
  `MetaTypes` space).  The second takes that state, the accumulated
  `From`, and the other parts of the join operation and returns a
  modified state and a `Join`.
* `map` is the same operation but specialized to `Unit` for the state.
* `reduce` is the same operation but only tracking the state and not
  making any changes to the structure of the `From`.

## Join

SoQL supports the join types `inner`, `left outer`, `right outer`, and
`full outer`, with the PostgreSQL `lateral` extension which makes the
right side of the join a function of the left side of the join.  a
`Join` object contains the type of the join, whether or not it's
lateral, the left side of the join (a `From`), the right side of the
join (an `AtomicFrom`) and the expression that filters the product of
the tables.

## AtomicFrom

An `AtomicFrom` has a table label, but is otherwise not particularly
interesting on its own.  Most of the time it is necessary to match to
one of its subclasses.

### FromTable

This represents a physical database table and knows that physical
table's name, its schema, and any primary keys the table may have.

### FromSingleRow

This is the most trivial `From` type; it represents a virtual table
with a single row and no columns, equivalent to an empty FROM or a
`FROM (SELECT)` in SQL.

### FromStatement

`FromStatement` represents a subquery and is not much more than a
`Statement` paired with a table label.

# Expression

Each expression node contains position information which includes both
an identifier which indicates which specific soql produced by the
FoundTables it came from as well as a position within that that soql.

`Expr` use the same sealed-split system as `Statement`.

## Column

### PhysicalColumn

This represents a reference to an actual physical database table.  For
example, in `SELECT x FROM @some_table`, `x` will be a `PhysicalColumn`.

### VirtualColumn

This represents a reference to a named intermediate value.  For
example, in `SELECT x FROM (SELECT x FROM @some_table)`, the outer
reference to `x` will be a `VirtualColumn` since it represents an
access to the subquery's output schema.

## SelectListReference

A `SelectListReference` is perhaps the most special kind of `Expr`.
In SQL, you can refer to select list columns by numerical index in
several positions.

`SelectListReference`s do not exist in the source SoQL form, nor are
they produced by analysis.  They are produced only by the
`SelectListReferences` rewrite pass.  Most rewrites assume they do not
exist, and the rewriting code will automatically undo/redo the pass in
order to perform other rewrites if necessary.

## Literal

### LiteralValue

A non-null soql value literal.

### NullLiteral

A typed null literal.

## FuncallLike

### FunctionCall

### AggregateFunctionCall

### WindowedFunctionCall

# Rollups

## Rollups as materialized views

The simplest move in which rollups can be used is as a materialized
view, where the "rollup" precomputes enough information to compute the
result of a (possibly different) query.

Ths bulk of this rollup is applied in `RollupExact#rewriteOneToOne`.

## Rollups as partially precomputed aggregations

Rollups can also be used to partially precompute aggregatations.  For
example, if you have a table with columns `a`, `b`, and `c`, and it
makes sense to group by `a` and `b` or by `b` and `c`, it might be a
good balance between ingress overhead and query runtime to create a
rollup that groups by all of `a`, `b`, and `c`.  This will produce
smaller groups than either of the two actually desired groups, but
will hopefully still performa a signficant chunk of the work required
to produce the desired coarser groups.  Then, when a query that groups
by one of the coarser sets is issued, if all agggregates can be
expressed in terms of rolling up the finer aggregates (hence the name
of the feature) the system can use the rollup to accelerate the query.

This rollup is applied in
`RollupExact#rewriteAggregatedOnAggregatedDifferentGroupBy`

Three interfaces in the rollup machinery can be implemented to perform
those further rollups in different ways: `SemigroupRewriter`,
`FunctionSplitter`, and `AdHocRewriter`.  In addition, the
`FunctionSubset` interface can be used to turn certain types of
expressions into "logical subgroups".

### SemigroupRewriter

This is perhaps the simplest interface; it has a single method which
takes a MonomorphicFunction and if that function is one that can be
further rolled up, it will return a function that takes an `Expr`
representing such a partial rollup and returns an `Expr` that
completes the rolling up.  For example, the count of multiple disjoint
sets can be combined into a count of the union of those sets by
summing the partial counts together.

### FunctionSplitter

Some functions can be "split" into sub-computations whose
sub-computations can be recombined into the total function.  For
example `avg(x)` can be split into `sum(x) / count(x)`, where the
sub-computations are `sum` and `count` and the recombining function is
`/`.  This trait expresses this kind of rewrite (and honestly, exists
purely to support `avg`)

### AdHocRewriter

This is the most general interface.  It turns an expression into a
collection of alternate equivalent expressions which may be easier to
roll up.  For example, if you have an expression like
`some_timestamp() < '2001-10-05T00:00:00``, this is the same as
`date_trunc_ymd(some_timestamp()) < '2001-10-05T00:00:00'` since the
`date_trunc_ymd` operation acts something like a `floor` function, and
as a result if you have a rollup which produces
`date_trunc_ymd(some_timestamp())` this rewrite opens the possibility
of of using that rollup.
