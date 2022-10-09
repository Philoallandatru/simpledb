package simpledb.optimizer;
import simpledb.optimizer.JoinOptimizer;
import simpledb.optimizer.LogicalJoinNode;

import java.util.List;

/** Class returned by {@link JoinOptimizer#computeCostAndCardOfSubplan} specifying the
    cost and cardinality of the optimal plan represented by plan.
*/
public class CostCard {
    /** The cost of the optimal subplan */
    public double cost;
    /** The cardinality of the optimal subplan */
    public int card;
    /** The optimal subplan */
    public List<LogicalJoinNode> plan;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Plan(").append(plan).append(")").append("\t").append("Cost(").append(cost).append(")").append("\t").append("card(").append(")");
        return sb.toString();
    }
}
