

package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */

    private Aggregator synAggregator;
    private OpIterator synItr;
    private Type synType;

    private OpIterator theChild;
    private int aField;
    private int gField;
    private Aggregator.Op aop;

    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {//
        // some code goes here
        theChild = child;
        aField = afield;
        gField = gfield;
        this.aop = aop;

        synType = (gField == -1 ? null : theChild.getTupleDesc().getFieldType(gField));

        //给aggregator赋值
        if (theChild.getTupleDesc().getFieldType(aField) == Type.INT_TYPE) {
            synAggregator = new IntegerAggregator(gField, synType, aField, this.aop);
        }
        else {
            if (theChild.getTupleDesc().getFieldType(aField) == Type.STRING_TYPE)
            {
                synAggregator = new StringAggregator(gField, synType, aField, this.aop);
            }
            else
                throw new IllegalArgumentException("Wrong in Aggregator! Type is not supported!");
        }
        //给迭代器赋值
        synItr=synAggregator.iterator();

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        if(gField==-1)
            return Aggregator.NO_GROUPING;
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if(gField==-1)
            return null;
        return theChild.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here

        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here

        return theChild.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        theChild.open();        //重要！！！！！！！
        while(theChild.hasNext())           //while!!!
        {
            synAggregator.mergeTupleIntoGroup(theChild.next());
        }

        synItr.open();
        super.open();           //!!!

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(synItr.hasNext())
        {
            return synItr.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        theChild.rewind();      //???这是为何
        synItr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor,
     * and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        List<Type> rtType=new ArrayList<>();
        List<String> rtName=new ArrayList<>();

        if(synType!=null)
        {
            rtType.add(synType);
            rtName.add(theChild.getTupleDesc().getFieldName(gField));
        }

        rtType.add(theChild.getTupleDesc().getFieldType(aField));
        rtName.add(theChild.getTupleDesc().getFieldName(aField));

        if(this.aop.equals(Aggregator.Op.SUM_COUNT))
        {
            rtType.add(Type.INT_TYPE);
            rtName.set(rtName.size()-1,"SUM");
            rtName.add("COUNT");
        }
        TupleDesc rtTd=new TupleDesc(rtType.toArray(new Type[rtType.size()]),rtName.toArray(new String[rtName.size()]));

        return rtTd;
    }

    public void close() {
        // some code goes here
        super.close();
        theChild.close();
        synItr.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{theChild};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        theChild = children[0];

        //不要忘了！！！
        synType = (gField == -1 ? null : theChild.getTupleDesc().getFieldType(gField));

        //给aggregator赋值
        if (theChild.getTupleDesc().getFieldType(aField) == Type.INT_TYPE) {
            synAggregator = new IntegerAggregator(gField, synType, aField, this.aop);
        } else {
            if (theChild.getTupleDesc().getFieldType(aField) == Type.STRING_TYPE) {
                synAggregator = new StringAggregator(gField, synType, aField, this.aop);
            } else
                throw new IllegalArgumentException("Wrong in Aggregator! Type is not supported!");
            //给迭代器赋值
            synItr = synAggregator.iterator();
        }
    }
}
