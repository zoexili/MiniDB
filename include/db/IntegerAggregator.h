#ifndef DB_INTEGERAGGREGATOR_H
#define DB_INTEGERAGGREGATOR_H

#include <db/Aggregator.h>
#include <optional>

namespace db {

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
    class IntegerAggregator : public Aggregator {
        int gbfield;
        int abfield;
        TupleDesc td;
        std::unordered_map<const Field *, int> count;
        std::unordered_map<const Field *, int> aggregates;
        Op what;
    public:
        /**
         * Aggregate constructor
         *
         * @param gbfield
         *            the 0-based index of the group-by field in the tuple, or
         *            NO_GROUPING if there is no grouping
         * @param gbfieldtype
         *            the type of the group by field (e.g., Type.INT_TYPE), or null
         *            if there is no grouping
         * @param afield
         *            the 0-based index of the aggregate field in the tuple
         * @param what
         *            the aggregation operator
         */

        IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield, Op what);

        /**
         * Merge a new tuple into the aggregate, grouping as indicated in the
         * constructor
         *
         * @param tup
         *            the Tuple containing an aggregate field and a group-by field
         */
        void mergeTupleIntoGroup(Tuple *tup) override;

        /**
         * Create a DbIterator * over group aggregate results.
         *
         * @return a DbIterator * whose tuples are the pair (groupVal, aggregateVal)
         *         if using group, or a single (aggregateVal) if no grouping. The
         *         aggregateVal is determined by the type of aggregate specified in
         *         the constructor.
         */
        DbIterator *iterator() const override;
    };
}

#endif