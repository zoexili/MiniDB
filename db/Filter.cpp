#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) : predicate(p), it(child) {
}

Predicate *Filter::getPredicate() {
    return &predicate;
}

const TupleDesc &Filter::getTupleDesc() const {
    return it->getTupleDesc();
}

void Filter::open() {
    Operator::open();
    it->open();
}

void Filter::close() {
    it->close();
    Operator::close();
}

void Filter::rewind() {
    it->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    return {it};
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    it = children[0];
}

std::optional<Tuple> Filter::fetchNext() {
    while (it->hasNext()) {
        auto tuple = it->next();
        if (predicate.filter(tuple)) {
            return tuple;
        }
    }
    return std::nullopt;
}
