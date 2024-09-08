#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) : p(p), child1(child1), child2(child2) {
    td = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    return &p;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    return td;
}

std::string HashEquiJoin::getJoinField1Name() {
    return child1->getTupleDesc().getFieldName(p.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    return child2->getTupleDesc().getFieldName(p.getField2());
}

void HashEquiJoin::open() {
    Operator::open();
    child1->open();
    child2->open();
}

void HashEquiJoin::close() {
    child1->close();
    child2->close();
    Operator::close();
}

void HashEquiJoin::rewind() {
    child1->rewind();
    child2->rewind();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    return {child1, child2};
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    child1 = children[0];
    child2 = children[1];
    td = TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    while (child1_tuple.has_value() || child1->hasNext()) {
        if (!child1_tuple.has_value()) {
            child1_tuple = child1->next();
        }
        while (child2->hasNext()) {
            Tuple innerTuple = child2->next();
            if (!p.filter(&child1_tuple.value(), &innerTuple)) {
                continue;
            }
            Tuple tuple(getTupleDesc());
            int i = 0;
            for (auto field: child1_tuple.value()) {
                tuple.setField(i, field);
                i++;
            }
            for (auto field :innerTuple) {
                tuple.setField(i, field);
                i++;
            }
            return tuple;
        }
        child1_tuple = std::nullopt;
        child2->rewind();
    }
    return std::nullopt;
}
