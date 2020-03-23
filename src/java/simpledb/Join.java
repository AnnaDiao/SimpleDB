package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate theP;
    private OpIterator childOne,childTwo;
    private Tuple remTup;
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        theP=p;
        childOne=child1;
        childTwo=child2;
        remTup=null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return theP;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return childOne.getTupleDesc().getFieldName(theP.getField1());
    }//WARNING: fieldNum存在theP里面！！！ 是fieldname不是属性那一串

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return childTwo.getTupleDesc().getFieldName(theP.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(childOne.getTupleDesc(),childTwo.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        childOne.open();
        childTwo.open();

    }

    public void close() {
        // some code goes here
        super.close();
        childOne.close();
        childTwo.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childOne.rewind();
        childTwo.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    // theP自带比较
    //因为一个tuple1有可能匹配多个tuple2，记得存档
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(remTup!=null || childOne.hasNext())
        {
            if(remTup==null)
                remTup=childOne.next();
            while(childTwo.hasNext())
            {
                Tuple tup2=childTwo.next();
                if(theP.filter(remTup,tup2))
                {
                    TupleDesc tmpTd=TupleDesc.merge(remTup.getTupleDesc(),tup2.getTupleDesc());
                    Tuple tmpTup=new Tuple(tmpTd);
                    tmpTup.setRecordId(remTup.getRecordId());
                    int st1=0;
                    while(st1<remTup.getTupleDesc().numFields())
                    {
                        tmpTup.setField(st1,remTup.getField(st1));
                        st1++;
                    }
                    int st2=0;
                    while (st2<tup2.getTupleDesc().numFields())
                    {
                        tmpTup.setField((st1+st2),tup2.getField(st2));
                        st2++;
                    }
                    // MAYBE
                    return tmpTup;
                }//看 Tuple.java
            }
            remTup=null;
            childTwo.rewind();
        }
        return null;
    }   //TO DO: 更高效        //

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{childOne,childTwo};
    }//????

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        childOne=children[0];
        childTwo=children[2];
    }

}
