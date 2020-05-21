package simpledb;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

public class BTreeReverseScan implements OpIterator{

    private static final long serialVersionUID = 1L;

    private boolean isOpen = false;
    private TransactionId tid;
    private TupleDesc myTd;
    private IndexPredicate ipred = null;
    private transient DbFileIterator it;
    private String tablename;
    private String alias;

    public BTreeReverseScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate ipred) {
        this.tid = tid;
        this.ipred = ipred;
        reset(tableid,tableAlias);
    }
    public BTreeReverseScan(TransactionId tid, int tableid, IndexPredicate ipred)
    {
        this(tid, tableid, Database.getCatalog().getTableName(tableid), ipred);
    }

    public String getTablename(){return this.tablename;}
    public String getAlias(){return this.alias;}

    public void reset(int tableid,String tableAlias)
    {
        this.isOpen=false;
        this.alias=tableAlias;
        this.tablename=Database.getCatalog().getTableName(tableid);
        if(ipred==null)
        {
            this.it= ((BTreeFile)Database.getCatalog().getDatabaseFile(tableid)).reverseIterator(tid);
        }
        else{
            //零一个指针
            this.it=((BTreeFile)Database.getCatalog().getDatabaseFile(tableid)).indexReverseIterator(tid,ipred);
        }
        myTd=Database.getCatalog().getTupleDesc(tableid);
        String[] newNames=new String[myTd.numFields()];
        Type[] newTypes = new Type[myTd.numFields()];
        for (int i = 0; i < myTd.numFields(); i++) {
            String name = myTd.getFieldName(i);
            Type t = myTd.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        myTd = new TupleDesc(newTypes, newNames);
    }
    @Override
    public void open() throws DbException, TransactionAbortedException, IOException {
        if(isOpen)
            throw new DbException("double open on one OpIterator.");
        it.open();
        isOpen=true;
    }


    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException, IOException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, IOException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");

        return it.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException, IOException {
        close();
        open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return myTd;
    }

    @Override
    public void close() {
        it.close();
        isOpen = false;
    }
}
