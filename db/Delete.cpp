#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) {
    tid = t;
    this->child = child;
    td = TupleDesc({Types::Type::INT_TYPE});
    done = true;
}

const TupleDesc &Delete::getTupleDesc() const {
    return td;
}

void Delete::open() {
    Operator::open();
    done = false;
    child->open();
}

void Delete::close() {
    child->close();
    Operator::close();
}

void Delete::rewind() {
    Operator::rewind();
    child->rewind();
}

std::vector<DbIterator *> Delete::getChildren() {
    return {child};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    child = children[0];
}

std::optional<Tuple> Delete::fetchNext() {
    if (done) return std::nullopt;
    done = true;
    int count = 0;
    BufferPool &bufferPool = Database::getBufferPool();
    while (child->hasNext()) {
        Tuple next = child->next();
        bufferPool.deleteTuple(tid, &next);
        count++;
    }
    Tuple result(td);
    result.setField(0, new IntField(count));
    return result;
}
