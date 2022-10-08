package simpledb.optimizer;

import simpledb.execution.Predicate;


/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {



    private final int buckets;
    private final int min;
    private final int max;
    private final double step;
    private final long[] counts;

    private int nTups;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        nTups = 0;
        this.min = min;
        this.max = max;
        step = Math.max(1, (max * 1.0 - min) / buckets);
        counts = new long[buckets];

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) return;
        int index = getIndex(v);
        long count = counts[index];
        counts[index] = count + 1;
        nTups += 1;
    }

    private int getIndex(int v) {
        int index = (int) Math.floor((v - min) / step);
        if (v >= max) index = buckets - 1;
        if (v <= min) index = 0;
        return index;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int index = getIndex(v);
        long equalCount = counts[index];

        if (op == Predicate.Op.EQUALS) {
            if (v > max || v < min) return 0;
            return equalCount / (nTups * step);
        }
        if (op == Predicate.Op.NOT_EQUALS) {
            if (v > max || v < min) return 1;
            return (nTups - equalCount * 1.0) / (nTups * step);
        }

        long largerCount = 0;
        int i = index + 1;
        while (i < buckets) {
            largerCount += counts[i++];
        }

        long lessCount = nTups - (largerCount + equalCount);

        if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (v < min) return 1.0;
            if (v > max) return 0;
            if (op == Predicate.Op.GREATER_THAN) return largerCount * 1.0 / (nTups * step);
            return (largerCount + equalCount*((min+step*(index+1)-v) / step)) / (nTups * step);
        }

        if (op == Predicate.Op.LESS_THAN_OR_EQ || op == Predicate.Op.LESS_THAN) {
            if (v > max) return 1.0;
            if (v < min) return 0;
            if (op == Predicate.Op.LESS_THAN) return lessCount * 1.0 / (nTups * step);
            return (lessCount + 1.0 * equalCount*((v + step -(min+step*index)) / step)) / (nTups * step);
        }
    	// some code goes here

        return -1.0;
    }


    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return  1.0 ;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buckets; i++) {
            sb.append("[").append(min).append(step * i).append("~").append(min).append(step * (i + 1)).append("]:").append(counts[i]).append("\t");
        }
        sb.append("\n");
        return sb.toString();
    }
}
