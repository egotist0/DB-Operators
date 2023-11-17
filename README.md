# Operators

Implement the following simplified physical operators by using the iterator model:

* Print: Prints tuples separated by newlines. Attributes should be separated
  with a comma without any extra spaces.
* Projection: Generates tuples with a subset of attributes.
* Select: Filters tuples by a given predicate. Each predicate consists of one
  relational operator (`l == r`, `l != r`, `l < r`, `l <= r`, `l > r`, `l >= r`)
  where `l` stands for an attribute and `r` stands for an attribute or a
  constant.
* Sort: Sorts tuples by the given attributes and direction (ascending or
  descending).
* HashJoin: Computes the inner equi-join of two inputs on one attribute.
* HashAggregation: Groups and calculates (potentially multiple) aggregates on
  the input.
* Union: Computes the union of two inputs with set semantics.
* UnionAll: Computes the union of two inputs with bag semantics.
* Intersect: Computes the intersection of two inputs with set semantics.
* IntersectAll: Computes the intersection of two inputs with bag semantics.
* Except: Computes the difference of two inputs with set semantics. (defaults to left tuple)
* ExceptAll: Computes the difference of two inputs with bag semantics.

To pass tuples between the operators, implement a `Register` class that
represents a single attribute. It should support storing 64 bit signed integers
and fixed size strings of length 16.

Add the implementation are in the files `src/include/operators/operators.h` and
`src/operators/operators.cc`. There you can find the `Register` class and one class for
each operator. 
