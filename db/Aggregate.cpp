#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    return aggregatorIt->hasNext() ? std::make_optional(aggregatorIt->next()) : std::nullopt;
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) {
    it = child;
    this->afield = afield;
    this->gfield = gfield;
    std::optional<Types::Type> gField;
    gfield != Aggregator::NO_GROUPING ? std::make_optional(it->getTupleDesc().getFieldType(gfield)) : std::nullopt;
    this->aop = aop;
    switch (it->getTupleDesc().getFieldType(afield)) {
        case Types::INT_TYPE:
            aggregator = new IntegerAggregator(gfield, gField, afield, aop);
            break;
        case Types::STRING_TYPE:
            aggregator = new StringAggregator(gfield, gField, afield, aop);
            break;
    }
}

int Aggregate::groupField() {
    return gfield;
}

std::string Aggregate::groupFieldName() {
    return gfield != Aggregator::NO_GROUPING ? it->getTupleDesc().getFieldName(gfield) : "";
}

int Aggregate::aggregateField() {
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    return it->getTupleDesc().getFieldName(afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    return aop;
}

void Aggregate::open() {
    Operator::open();
    it->open();
    while (it->hasNext()) {
        Tuple next = it->next();
        aggregator->mergeTupleIntoGroup(&next);
    }
    it->close();
    aggregatorIt = aggregator->iterator();
    aggregatorIt->open();
}

void Aggregate::rewind() {
    Operator::rewind();
    aggregatorIt->rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
    return aggregatorIt->getTupleDesc();
}

void Aggregate::close() {
    aggregatorIt->close();
    Operator::close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    return {it};
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    it = children[0];
}
