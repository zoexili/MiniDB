#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    int gbfield;
    const TupleDesc &td;
    Aggregator::Op what;
    std::unordered_map<const Field *, int> count;
    std::unordered_map<const Field *, int> aggregates;
    std::unordered_map<const Field *, int>::iterator it;
public:
    IntegerAggregatorIterator(int gbfield,
                              const Aggregator::Op what,
                              const TupleDesc &td,
                              const std::unordered_map<const Field *, int> &aggregates,
                              const std::unordered_map<const Field *, int> &count) :
            gbfield(gbfield), what(what), td(td), aggregates(aggregates), count(count) {
    }

    void open() override {
        it = aggregates.begin();
    }

    bool hasNext() override {
        return it != aggregates.end();
    }

    Tuple next() override {
        auto next = *it;
        ++it;
        Tuple tuple(td);
        if (gbfield != Aggregator::NO_GROUPING) {
            tuple.setField(0, next.first);
            tuple.setField(1, new IntField(next.second));
        } else {
            tuple.setField(0, new IntField(next.second));
        }
        return tuple;
    }

    void rewind() override {
        close();
        open();
    }

    const TupleDesc &getTupleDesc() const override {
        return td;
    }

    void close() override {
        it = aggregates.end();
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what) {
    this->gbfield = gbfield;
    this->abfield = afield;
    this->what = what;
    std::vector<Types::Type> types = {Types::Type::INT_TYPE};
    if (gbfield != Aggregator::NO_GROUPING) {
        types.insert(types.begin(), gbfieldtype.value());
    }
    td = TupleDesc(types);
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    const Field *gbField = gbfield != NO_GROUPING ? &tup->getField(gbfield) : nullptr;
    const auto &aField = dynamic_cast<const IntField &>(tup->getField(abfield));
    switch (what) {
        case Op::MIN:
            if (aggregates.find(gbField) == aggregates.end()) {
                aggregates[gbField] = aField.getValue();
            } else {
                aggregates[gbField] = std::min(aggregates[gbField], aField.getValue());
            }
            break;
        case Op::MAX:
            if (aggregates.find(gbField) == aggregates.end()) {
                aggregates[gbField] = aField.getValue();
            } else {
                aggregates[gbField] = std::max(aggregates[gbField], aField.getValue());
            }
            break;
        case Op::AVG:
            if (count.find(gbField) == count.end()) {
                count[gbField] = aField.getValue();
            } else {
                count[gbField] += aField.getValue();
            }
        case Op::SUM:
            if (aggregates.find(gbField) == aggregates.end()) {
                aggregates[gbField] = aField.getValue();
            } else {
                aggregates[gbField] += aField.getValue();
            }
            break;
        case Op::COUNT:
            if (aggregates.find(gbField) == aggregates.end()) {
                aggregates[gbField] = 1;
            } else {
                aggregates[gbField] += 1;
            }
            break;
    }
}

DbIterator *IntegerAggregator::iterator() const {
    return new IntegerAggregatorIterator(gbfield, what, td, aggregates, count);
}
