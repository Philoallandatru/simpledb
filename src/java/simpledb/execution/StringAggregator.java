package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private Tuple result;
    private int count;
    /* contains all the aggregated tuples of certain group, key is group field */
    private Map<Field, Tuple> aggregates;

    /* used to compute average and count, so you need the number of tuples in each group */
    private Map<Field, Integer> groupCount;


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.result = null;
        this.count = 0;
        this.aggregates = new HashMap<>();
        this.groupCount = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            if (result == null) {
                Type fieldType = tup.getTupleDesc().getFieldType(afield);
                String fieldName = tup.getTupleDesc().getFieldName(afield);
                TupleDesc td = new TupleDesc(new Type[]{fieldType}, new String[]{fieldName});
                result = new Tuple(td);
                result.setField(0, new IntField(1));
                count = 1;
            } else {
                result.setField(0, new IntField(count + 1));
                count += 1;
            }
        } else {
            int gb = 0;
            int agg = 1;
            if (aggregates.isEmpty()) {
                addNewGroupByTuple(tup);
            } else {
                /* note that IntField has already implemented equals method, so use hashmap here  */
                Field groupByField = tup.getField(gbfield);
                if (aggregates.containsKey(groupByField)) {
                    Tuple tuple = aggregates.get(groupByField);
                    Integer currCount = groupCount.get(groupByField);
                    tuple.setField(agg, new IntField(currCount + 1));
                    groupCount.put(groupByField, currCount + 1);
                } else {
                    addNewGroupByTuple(tup);
                }
            }
        }
    }

    /**
     * add a new tuple to the hashmap, which does not belong to
     * any of existing group tuple.
     * @param tup
     */
    private void addNewGroupByTuple(Tuple tup) {
        Type afieldType = tup.getTupleDesc().getFieldType(afield);
        String afieldName = tup.getTupleDesc().getFieldName(afield);
        String gbFieldName = tup.getTupleDesc().getFieldName(gbfield);
        TupleDesc td = new TupleDesc(
                new Type[]{gbfieldtype, Type.INT_TYPE},
                new String[]{gbFieldName, afieldName}
        );
        /* create first tuple in aggregate */
        Tuple first = new Tuple(td);
        /* set  */
        first.setField(0, tup.getField(gbfield));
        first.setField(1, new IntField(1));
        aggregates.put(tup.getField(gbfield), first);
        groupCount.put(tup.getField(gbfield), 1);
    }



    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            return new OpIterator() {
                private Iterator<Tuple> it;
                @Override
                public void open() throws DbException, TransactionAbortedException {
                    ArrayList<Tuple> list = new ArrayList<>();
                    list.add(result);
                    it = list.iterator();
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return it.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if (it == null || !it.hasNext()) return null;
                    return it.next();
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                }

                @Override
                public void close() {
                    it = null;
                }
            };
        } else {
            return new OpIterator() {
                private Iterator<Tuple> it;
                @Override
                public void open() throws DbException, TransactionAbortedException {
                    it = aggregates.values().iterator();
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return it.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if (it == null || !it.hasNext()) return null;
                    return it.next();
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
                }

                @Override
                public void close() {
                    it = null;
                }
            };
        }
    }

}
