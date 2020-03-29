package simpledb;

import com.sun.deploy.security.SelectableSecurityManager;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private OpIterator thechild;
    private int tableId;
    private int called;
    private TupleDesc theTd;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid =t;
        this.thechild=child;
        this.tableId=tableId;
        this.called=0;
        this.theTd=new TupleDesc(new Type[]{Type.INT_TYPE});

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //看定义！
        //return this.thechild.getTupleDesc();
        return this.theTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.thechild.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.thechild.close();
        this.called=0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        this.rewind();
        this.called=0;

    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    // 重要！！！ 不是一个tp 是一整串！
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called==1)
            return null;
        called=1;
        int cnt=0;
        while (this.thechild.hasNext())
        {
            Tuple tmpTp=thechild.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, tmpTp);
                cnt++;
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }       // 记得加Try!!!!!!!不要试图改源码
        }

        Tuple rtTp=new Tuple(this.theTd);
        //类型转换！！！！！！！！！！！！！
        IntField tmpf=new IntField(cnt);
        rtTp.setField(0,tmpf);
        return rtTp;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.thechild};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.thechild=children[0];
    }
}
