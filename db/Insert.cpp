#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    if (done) return std::nullopt;
    done = true;
    int count = 0;
    BufferPool &bufferPool = Database::getBufferPool();
    while (child->hasNext()) {
        Tuple next = child->next();
        bufferPool.insertTuple(transactionId, tableId, &next);
        count++;
    }
    Tuple result(td);
    result.setField(0, new IntField(count));
    return result;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) {
    transactionId = t;
    this->child = child;
    this->tableId = tableId;
    td = TupleDesc({Types::Type::INT_TYPE});
}

const TupleDesc &Insert::getTupleDesc() const {
    return td;
}

void Insert::open() {
    child->open();
    done = false;
    Operator::open();
}

void Insert::close() {
    child->close();
    Operator::close();
}

void Insert::rewind() {
    child->rewind();
    Operator::rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
    return {child};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    child = children[0];
}
