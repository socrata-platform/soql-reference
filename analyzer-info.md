Why is Analyzer Even
====================

Quick Historical Platform Overview
----------------------------------

* Core + userdbs - soda1
   - userdbs not really scalable (every dataset in exactly 1 pg instance)
   - soda1 really not nice as an API
* soda2!  SQL-like!  Simple JSON that demos well!
* New architecture
   - soda-fountain: main API entry point
       - intended to (but never did) become the public server
           - if it were to, it would at least need to know about dataset namespaces, views, users.
           - It knows none of this.
           - but this is why datasets are indexed via a "resource name"
       - mainly dispatches requests to other parts of the system
   - data-coordinator: where datasets live
       - this is a staging point - data is uploaded to here, then distributed
       - authoritative "state of the world" which is why its database is called "truth"
       - datasets at this level are addressed by their "internal name" which
         indicates which truth it lives in ("alpha.1234")
   - secondary-watcher: observes truths and puts datasets into secondaries
       - secondary_manifest table in truth tracks which datasets live where
       - some secondary watchers are "feedback secondaries" which observe updates
         and then send new data into truth.  (this is how geo-region columns work)
   - query-coordinator: query dispatch logic
       - the intent, never fully realized, was for this to look at your
         query and dispatch it to an appropriate secondary for processing.
       - but we never really grew any SoQL-speaking secondaries other than pg
       - so now it just analyzes your query and dispatches it to a pg
         secondary that contains all datasets referenced by it.
       - this also does rollup-rewriting.  Unsure if this is the right place for that!
   - soql-server-pg
       - Turns analyses from query-coordinator into SQL.  That's
         really about it!

Where does analyzer fit in?
---------------------------

* soql-analyzer takes a soql query, typechecks it, and produces a data
  structure which is theoretically easy to use for other purposes.
* An analysis is one-way; the result of it is not necessarily easy (or even
  possible) to turn back into text.
* If the analyzer is doing it's job, there should be very little "implicit"
  knowledge left in the analysis that needs to be reconstructed.
* It's used a little in core (honestly should be more, it has a partial
  reimplementation of the analyzer that does not handle overloaded functions
  correctly all the time) but mainly in query-coordinator and soql-server-pg

History of the analyzer, part 1: names
--------------------------------------

* soda2 was designed with a particular set of goals on both the query
  and result side.
* On both sides, simplicity was king
* On the result side, an API result would be a JSON array containing
  objects, one object per row.
   - infelicity #1: this leaves no place for the row's schema
      - it must be inferred from the result if you don't know the
         semantics of the query you're issuing
      - we tried to work around this by putting schema information
         in the HTTP result headers, but this _very_ quickly runs into
         header size limitations.
* On the query side, queries would be made with HTTP GETs containing
  soql in the query-part.
   - this isn't 100% true, but the alternatives were more limited and
     while they still exist are largely forgotten.  Ask me later
     if you're curious.
   - infelicity #2: HTTP GET also has fairly strict length limits
* So the result of these two design decisions led to a couple of ways
  in which SoQL does not behave like SQL.
   - First, the "JSON object" requirement means that all columns in
     a result must have different names.
   - Second, the "HTTP GET" leads to wanting to reduce the amount of
     duplication in query strings.
   - as a result, SoQL has the following (mis?)features:
      - if you don't give a selected expression a name, one will be
        generated for you.
      - you can use the names of selected expressions later in your
        query.
      - for example you can do `select x * y where x_y > 5` and it's
        the same as `select x * y as x_y where x * y > 5`.
      - these transformations are part of what the analyzer does.

History of the analyzer, part 2: joins
--------------------------------------

* Back in The Day of soda1, datasets and views formed a tree
* soda2 as initially designed mirrored this; the assumption
  was that every query would be executed in the "context" of
  its parent view and no more information would be required.
  - This is the origin of the `|>` piping operator.  We would
    build a query that looks like `select * |> view1 |> view2`
* This is why soql has (well, had) no `from` clause!
* With the introduction of joins comes the necessity to specify
  what table a column-reference comes from.
   - but we were already using `x.y` syntax to mean something
     different!
   - this is why the @-sigils were introduced.
* Joins also fairly quickly brought subqueries, which brought
  the notion of namespacing of table- and column-names.

Where the old analyzer stumbles and falls
-----------------------------------------

* Error messages aren't great
  - you give the old analyzer a single string which it parses and
    checks.
      - this means if you want to merge multiple soql snippets
        (for example if you're querying a view which joins to
        another view which is a query on some dataset...) reporting
        errors in a reasonable way becomes tricky.
      - originally when queries were a simple pipe-chain this was
        tractable (..not that we actually did it..) but with
        subqueries being merged every-which-way it quickly becomes
        quite hard.
  - `FROM` was not really added in a principled way.
      - in SQL, the `FROM` clause defines the table you're querying.
         - it includes the JOINs
      - in SoQL, FROM is a separate thing, because JOINs actually
         came first!
* I'm throughly unconvinced the analyzer tracks the sources of
  names correctly, and it _definitely_ doesn't produce an analysis
  with all the information it knows when it does.
    - Column-references in an analysis may be unqualified, which
      means later processing has to do this tracking itself!
    - Where it _is_ qualified, it's qualified with names provided
      by the user, so later processing has to track namespacing too!
* It conflates |> and the table-set operations (`UNION` et al) in its
  `BinaryTree` structure (this is more an ergonomics thing than a
  correctness thing; these _do_ in fact form a binary tree but the
  interpretation of it is weird)
* Window functions are very unstructured and their relationship
  with aggregates is not clear.
* It doesn't know about UDFs at all, this is managed in core by
  preprocessing the query (with similar but lesser problems to
  those of user parameters being handled in this way)

So, the NEW ANALYZER.  How is it different from the old?
--------------------------------------------------------

* There is a preparation step before the analyzer is involved.
  - I've introduced a "TableFinder" which responsible for going
    from "what the user entered" to "here are all the queries that
    are (transitively) referenced by that query" without losing the
    identities of those transitive queries.
  - The output of a TableFinder is a FoundTables, which is
    serializable.  Core will be responsible for producing one.
* An analysis no longer privileges the context table.
  - All `Select`s have a proper `FROM` which is much more SQL-like.
* The analyzer knows about UDFs.
* All tables and columns are given unambiguous labels and all column
    references are fully qualified in the output.
* An analysis's output is a Statement, which is a case class:
  - it can be a `Select`, or a `CombinedTables`, or even a `Values` or
    a `CTE` (neither of which can be represented in SoQL, though
    `Values` will sometimes be generated as part of UDF processing)
* During processing, the analyzer keeps much more close track of
    the current environment in which name can appear.
  - I'm hoping to use this to implement `IN (subquery)` and CTE
    support sometime.
* Errors are positioned within the original-as-provided-by-the-user
  query (at least, the ones which are positioned at all - the parse
  tree doesn't yet provide enough information for all errors.  I
  didn't want to touch that layer yet because parsing is done by
  more things than analyze).
* Query merge is an optional postprocessing step, and only one of
  several.  Because of the new labelling system, it is much easier
  to make changes post-analysis
     - one such (still a work-in-progress) attempts to preserve
       ordering across queries, which is a thing that has plagued
       frontend's pagination for a while.
* Because the new analyzer knows which part of the query it's
  processing, `param` calls don't need to (but still can) specify
  which dataset they're referring to!

I want to see the code!
-----------------------

* It's on the `rjm/analyzer2` branch of soql-reference
* Here are the main classes
  - The `TableFinder` is an abstract class which needs to be subclassed
     in order to teach it how to find a table, view, or udf in some
     scope given some name.
    - a "scope" is undefined beyond "can be used as a
      hash table key"; the intent is that federation and
      user privilege info be part of the scope.
  - This produces a `FoundTables`, which is opaque outside
    the analyzer, beyond being JSON-serializable.
     - A FoundTables can be deserialized as an UnparsedFoundTables
       if it wants to pass through a thing that doesn't care about
       the parse tree (e.g., soda-fountain)
  - A `SoQLAnalyzer` is a function from FoundTables and a set of
    user-parameters, producing either an analysis or an error.
  - The `SoQLAnalysis` contains a label provider and a statement
    and can be used to postprocess the statement to do things
    like merging queries together.
     - as before, `SoQLAnalysis` can be serialized into a versioned
       binary blob; implementing this (de)serialization is way nicer
       in the new analyzer than the old.
  - The `Statement` in the analysis is mostly made up of `Expression`s
     and `From` (which themselves contain more `Statement`s)

What the new analyzer doesn't fix (a probably incomplete list)
--------------------------------------------------------------
* Alias-generation works exactly the same
  - The implicitly generated alias for a qualified column reference
      leaves off the qualifier.
   - e.g., "select @foo.bar ...." will produce a column named "bar" in its result
   - this is different from every other piece of syntax in an expression.
   - but it can't be changed without breaking existing queries.
* You can use implicitly-generated aliases later in the query (either
  in the same query, or outside it when it's the output of a subquery)
   - this mostly works fine (and changing it would be breaking)
   - but it's fragile (_any_ schema change can alter the names generated)

Known changes in the new analyzer that I'm hoping will not be big deals
-----------------------------------------------------------------------
* Because of a chicken-and-egg issue, the new analyzer does NOT allow
  using aliases from the select list in the FROM+JOIN parts of the query.
