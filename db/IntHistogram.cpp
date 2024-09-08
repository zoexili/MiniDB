#include <db/IntHistogram.h>
#include <string>
#include <sstream>
#include <iostream>

using namespace db;

IntHistogram::IntHistogram(int buckets, int min, int max) {
    this->buckets = buckets;
    this->min = min;
    this->max = max;
    if ((max - min + 1) / buckets < 1) {
        this->width = 1;
    }
    else {
        this->width = (max - min + 1) / buckets;
    }
    this->numTuples = 0;
    mybuckets.resize(buckets);
}

void IntHistogram::addValue(int v) {
    // find the bucket v belongs to
    int pos = (v - min) / width;
    // increase bucket height by 1
    mybuckets[pos] += 1;
    numTuples += 1;
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    int pos;
    if (v == max) {
        pos = buckets - 1;
    } else {
        pos = (v - min) / width;
    }

    int height;
    int left = pos * width + min;
    int right = pos * width + min + width;

    switch (op) {
        case Predicate::Op::LIKE:
            std::cout << "like not supported, replaced by equals" << std::endl;
        case Predicate::Op::EQUALS: {
            if (v < min || v > max) {
                return 0.0;
            } else {
                height = mybuckets[pos];
                return (height * 1.0 / width) / numTuples;
            }
        }
        case Predicate::Op::GREATER_THAN: {
            if (v < min) {
                return 1.0;
            }
            if (v > max) {
                return 0.0;
            }
            height = mybuckets[pos];
            // std::cout << "height: " << height << std::endl;
            // std::cout << "numTuples: " << numTuples << std::endl;
            double p1 = ((right - v) * 1.0 / width * 1.0) * (height * 1.0 / numTuples);
            // std::cout << "p1: " << p1 << std::endl;
            int allInRight = 0;
            for (int i = pos + 1; i < buckets; i++) {
                allInRight += mybuckets[i];
            }
            // std::cout << "allInRight: " << allInRight << std::endl;
            double p2 = allInRight * 1.0 / numTuples;
            // std::cout << "p2: " << p2 << std::endl;
            // std::cout << "p1 + p2: " << p1+p2 << std::endl;

            if (width == 1) {
                return p2;
            } else {
                return p1+p2;
            }
        }
        case Predicate::Op::LESS_THAN: {

            if (v < min) {
                return 0.0;
            }
            if (v > max) {
                return 1.0;
            }
            height = mybuckets[pos];
            double pp1 = ((v - left) * 1.0 / width * 1.0) * (height * 1.0 / numTuples);
            int allInLeft = 0;
            for (int i = pos - 1; i >= 0; i--) {
                allInLeft += mybuckets[i];
            }
            double pp2 = allInLeft * 1.0 / numTuples;
            return pp1 + pp2;
        }
        case Predicate::Op::LESS_THAN_OR_EQ: {
            return estimateSelectivity(Predicate::Op::LESS_THAN, v) + estimateSelectivity(Predicate::Op::EQUALS, v);
        }
        case Predicate::Op::GREATER_THAN_OR_EQ: {
            return estimateSelectivity(Predicate::Op::GREATER_THAN, v) + estimateSelectivity(Predicate::Op::EQUALS, v);
        }
        case Predicate::Op::NOT_EQUALS: {
            return 1 - estimateSelectivity(Predicate::Op::EQUALS, v);
        }
    }
}

double IntHistogram::avgSelectivity() const {
    return 1.0;
}

std::string IntHistogram::to_string() const {
    std::ostringstream s;
    for (int i = 0; i < buckets; i++) {
        s << "bucket " << i << ": ";
        for (int j = 0; j < mybuckets[i]; j++) {
            s << "|";
        }
        s << "\n";
    }
    return s.str();
}
