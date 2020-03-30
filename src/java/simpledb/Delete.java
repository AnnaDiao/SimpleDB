package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId tid;
    private OpIterator thechild;
    private TupleDesc theTd;
    private int cnt;
    private int called;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid=t;
        this.thechild=child;
        this.theTd=new TupleDesc(new Type[]{Type.INT_TYPE});
        this.cnt=0;
        this.called=0;
        this.called=0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
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
        this.cnt=0;
        this.called=0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.thechild.rewind();;
        this.cnt=0;
        this.called=0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called==1)
            return null;
        called=1;
        while (this.thechild.hasNext())
        {
            Tuple tmpTp=thechild.next();
            try {
                Database.getBufferPool().deleteTuple(this.tid,  tmpTp);
                cnt++;
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }       // 记得加Try!!!!!!!不要试图改源码
        }       // 吐血。 虽然没说只能call一次，但就是只能call一次

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
