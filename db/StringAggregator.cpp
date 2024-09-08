#include <db/StringAggregator.h>

using namespace db;

StringAggregator::StringAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield, Aggregator::Op what) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + " not implemented");
}

void StringAggregator::mergeTupleIntoGroup(Tuple *tup) {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + " not implemented");
}

DbIterator *StringAggregator::iterator() const {
    throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + " not implemented");
}
