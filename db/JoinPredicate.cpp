#include <db/JoinPredicate.h>
#include <db/Tuple.h>

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2) {
    this->field1 = field1;
    this->op = op;
    this->field2 = field2;
}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
    return t1->getField(field1).compare(op, &t2->getField(field2));
}

int JoinPredicate::getField1() const {
    return field1;
}

int JoinPredicate::getField2() const {
    return field2;
}

Predicate::Op JoinPredicate::getOperator() const {
    return op;
}
