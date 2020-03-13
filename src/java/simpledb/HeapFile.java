package simpledb;

import jdk.internal.org.objectweb.asm.tree.TryCatchBlockNode;

import java.io.*;
import java.util.*;
import java.io.RandomAccessFile;
/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     *            后备/备份存储器
     *            file就是磁盘上存heapfile的地方
     */
    private File oneFile;
    private TupleDesc tpDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.oneFile=f;
        this.tpDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return oneFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return oneFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tpDesc;
    }
    /**
     * Read the specified page from disk.
     *
     * throws IllegalArgumentException if the page does not exist in this file.
     */
    // see DbFile.java for javadocs
    //file 存了 page
    //https://blog.csdn.net/qq_21808961/article/details/80187662 磁盘读写
    public Page readPage(PageId pid){
        // some code goes here
        try{
            RandomAccessFile tmpFile=new RandomAccessFile(oneFile,"r");
            int pgNo=pid.getPageNumber();
            int pageSize=BufferPool.getPageSize();
            if(pageSize*(pgNo+1)>oneFile.length())         //页码从0开始
            {
                tmpFile.close();
                throw new IllegalArgumentException("Wrong in HeapFile! pgNo does no exist!");
            }

            byte[] bytes = new byte[pageSize];

            tmpFile.seek(pgNo*pageSize);
            tmpFile.read(bytes);
            //只能调用继承，不太懂
            HeapPageId rtId= new HeapPageId(pid.getTableId(),pid.getPageNumber());
            HeapPage rtPage=new HeapPage(rtId,bytes);

            return rtPage;
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }// TO DO

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor(oneFile.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    //返回tuple
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new fileItr(tid);
    }
    //TransactionId x //调bufferpool需要pageid类型
    private class fileItr implements DbFileIterator{
        //调用page的Itr
        int pageNo;
        private Iterator<Tuple> pageTupleItr;

        private HeapFile file;
        private TransactionId tid;

        fileItr(TransactionId tid)
        {
            this.tid=tid;
            pageNo=-1;
            pageTupleItr=null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo=0;
            HeapPageId tmpPgId=new HeapPageId(getId(),pageNo);      //见HeapPageId的声名
            pageTupleItr=((HeapPage) Database.getBufferPool().getPage(tid,tmpPgId,Permissions.READ_ONLY)).iterator();

        }

        public boolean hasNext() throws TransactionAbortedException, DbException {
            if(pageNo==-1)
                return false;
            if(pageTupleItr.hasNext())
                return true;
            if(pageNo<numPages()-1)
            {
                pageNo++;
                HeapPageId tmpPgId=new HeapPageId(getId(),pageNo);
                pageTupleItr=((HeapPage)Database.getBufferPool().getPage(tid,tmpPgId,Permissions.READ_ONLY)).iterator();
                return pageTupleItr.hasNext();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
            {
                throw new NoSuchElementException("Wrong in HeapFile! No next Tuple!");
            }
            return pageTupleItr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
                open();
        }

        @Override
        public void close() {
            pageNo=-1;
            pageTupleItr=null;
        }
    }
}

