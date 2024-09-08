#include <db/JoinOptimizer.h>
#include <db/PlanCache.h>
#include <cfloat>

using namespace db;

double JoinOptimizer::estimateJoinCost(int card1, int card2, double cost1, double cost2) {
    // cost is scan cost, card is cardinality=nTups
    return cost1 + card1 * cost2 + card1 * card2;
}

int JoinOptimizer::estimateTableJoinCardinality(Predicate::Op joinOp,
                                                const std::string &table1Alias, const std::string &table2Alias,
                                                const std::string &field1PureName, const std::string &field2PureName,
                                                int card1, int card2, bool t1pkey, bool t2pkey,
                                                const std::unordered_map<std::string, TableStats> &stats,
                                                const std::unordered_map<std::string, int> &tableAliasToId) {
    if (joinOp == Predicate::Op::EQUALS) {
        if (t1pkey) {
            return card2;
        } 
        else if (t2pkey) {
            return card1;
        }
        else if (card1 >= card2) {
            return card1;
        }
        else {
            return card2;
        }
    } 
    else {
        return (int) 0.3 * card1 * card2;
    }
}

std::vector<LogicalJoinNode> JoinOptimizer::orderJoins(std::unordered_map<std::string, TableStats> stats,
                                                       std::unordered_map<std::string, double> filterSelectivities) {
    // joins is the vector of logical join nodes
    int numJoinNodes = joins.size();
    PlanCache memo;
    std::unordered_set<LogicalJoinNode> wholeSet;
    for (int i = 0; i <= numJoinNodes; i++) {
        std::vector<std::unordered_set<LogicalJoinNode>> setOfSubset = enumerateSubsets(joins, i);
        for (std::unordered_set<LogicalJoinNode> s : setOfSubset) {
            if (s.size() == numJoinNodes) {
                wholeSet = s;
            }
            // set bestcostsofar to max_value
            double bestCostSoFar = std::numeric_limits<double>::max();
            CostCard * bestPlan; // set bestPlan to pointer value
            for (LogicalJoinNode toRemove : s) {
                CostCard *plan = computeCostAndCardOfSubplan(stats, filterSelectivities, toRemove, s, bestCostSoFar, memo);
                if (plan != nullptr) {
                    bestCostSoFar = plan->cost;
                    bestPlan = plan;
                }
            }
            memo.addPlan(s, bestPlan);
        }
    }
    return memo.get(wholeSet)->plan;
}
