package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     *            //特殊处理
     */
    private TransactionId scanTid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator dbfItr;  //因为要调用定义的函数，不能设置为 Iterator<Tuple>
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.scanTid=tid;
        this.tableId=tableid;
        this.tableAlias=tableAlias;
        this.dbfItr=null;
    }//还没有处理null

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
        //异常在Catalog里处理完了
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias=tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        dbfItr=Database.getCatalog().getDatabaseFile(tableId).iterator(scanTid);
        dbfItr.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     *         //每个小名字都要改
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        String foreStr;
       // StringBuilder tmpStr = new StringBuilder();//JDK1.5版本提供的类，线程不安全，不做线程同步检查，因此效率较高。 建议采用该类。
        if(tableAlias==null)
        {
            foreStr="null";
           // tmpStr.append("null.");
        }
        else
        {
            //tmpStr.append(tableAlias);
           // tmpStr.append(".");
            foreStr=tableAlias+".";
        }
        TupleDesc tmpTpdesc=Database.getCatalog().getTupleDesc(tableId);
        int pos=tmpTpdesc.numFields();
        List<Type> tmpType = new ArrayList<>();
        List<String> tmpName=new ArrayList<>();
        for(int i=0;i<pos;i++)
        {
            tmpType.add(tmpTpdesc.getFieldType(i));     //append 类型

            String dropName=tmpTpdesc.getFieldName(i);  //判断名字是否为空
            StringBuilder tmpStr = new StringBuilder();
            tmpStr.append(foreStr);
            if(dropName==null)
            {
                tmpStr.append("null");
            }
            else
            {
                tmpStr.append(dropName);
            }

            tmpName.add(tmpStr.toString());
        }

        return new TupleDesc((Type[])tmpType.toArray(),(String[]) tmpName.toArray());
    }//what。。。//就是把表名改了

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(dbfItr==null)
            throw new NullPointerException("Wrong in Scan! Itr hasn't been open yet.");// 看看能改成什么报错比较好
        return dbfItr.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(!hasNext())
            throw new NoSuchElementException("Wrong in Scan! No next tuple.");
        return dbfItr.next();//异常处理在DBfile里
    }

    public void close() {
        // some code goes here
        dbfItr=null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        dbfItr.rewind();//.open应该同理
    }
}
