package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    private Predicate theP;
    private OpIterator theChild;
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.theP=p;
        this.theChild=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return theP;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return theChild.getTupleDesc();
    }   //什么原理

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException, IOException, InterruptedException {
        // some code goes here
        super.open();
        theChild.open();
    }   //Filter归属于operator, 要记得开父类的指针！！！

    public void close() {
        // some code goes here
        super.close();
        theChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException, IOException, InterruptedException {
        // some code goes here
        //super.rewind();
        theChild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples
     * from the child operator,         //从child开始遍历！
     * applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException, IOException, InterruptedException {
        // some code goes here

        while(theChild.hasNext())
        {
            Tuple tmpt=theChild.next();
            if(theP.filter(tmpt))
            {
                return tmpt;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{theChild};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        theChild=children[0];
    }

}
