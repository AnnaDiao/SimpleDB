
package simpledb;

import jdk.internal.org.objectweb.asm.tree.TryCatchBlockNode;

import java.io.*;
import java.lang.reflect.Array;
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
        int pgNo=page.getId().getPageNumber();
        try{
            if(pgNo>numPages())
            {
                throw new IllegalArgumentException();
            }
            RandomAccessFile tmpFile=new RandomAccessFile(oneFile,"rw");    //跟下面一致
            tmpFile.seek(pgNo*(BufferPool.getPageSize()));
            // 见paga.java
            byte[] bytes=page.getPageData();
            tmpFile.write(bytes);
            tmpFile.close();

        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor(oneFile.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    //不要忘记从BufforPool来取!!!!
    //用Databse.bufforpool
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, InterruptedException {
        // some code goes here

        ArrayList<Page> rtPage = new ArrayList<>();
        HeapPage tmpPage = null;

        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(this.getId(), i);  //!!!!!
            tmpPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);   //不要用原型！会少函数！！！！
            if (!(tmpPage.getNumEmptySlots() == 0)) {
                tmpPage.insertTuple(t);
                rtPage.add(tmpPage);
                return rtPage;
            } else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        //重要！！！ 不够了还可以新开一个page！
        //throw new DbException("can not insert new tuple");
        // not necessary for lab1
        if (tmpPage == null || tmpPage.getNumEmptySlots() == 0) {
            byte[] emptypage = HeapPage.createEmptyPageData();

            //https://blog.csdn.net/merry3602/article/details/7045515/ 用字节流
            try {
                //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
                FileOutputStream writer = new FileOutputStream(oneFile, true);
                writer.write(emptypage);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            HeapPageId tmpPid = new HeapPageId(getId(), numPages() - 1); //序号从0！！！！！！
            tmpPage = (HeapPage) Database.getBufferPool().getPage(tid, tmpPid, Permissions.READ_WRITE);
        }
        tmpPage.insertTuple(t);
        rtPage.add(tmpPage);
        return rtPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException, InterruptedException {
        // some code goes here
        if(t.getRecordId().getPageId().getPageNumber()>numPages())
            throw new DbException("Tuple not belong to this file");

        ArrayList<Page> rtPage=new ArrayList<>();
        HeapPage page=(HeapPage)Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        page.deleteTuple(t); //自己就可以抛异常
        rtPage.add(page);
        return rtPage;
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
        public void open() throws DbException, TransactionAbortedException, IOException, InterruptedException {
            pageNo=0;
            HeapPageId tmpPgId=new HeapPageId(getId(),pageNo);      //见HeapPageId的声名
            pageTupleItr=((HeapPage) Database.getBufferPool().getPage(tid,tmpPgId,Permissions.READ_ONLY)).iterator();

        }

        public boolean hasNext() throws TransactionAbortedException, DbException, IOException, InterruptedException {
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
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, IOException, InterruptedException {
            if(!hasNext())
            {
                throw new NoSuchElementException("Wrong in HeapFile! No next Tuple!");
            }
            return pageTupleItr.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException, IOException, InterruptedException {
            open();
        }

        @Override
        public void close() {
            pageNo=-1;
            pageTupleItr=null;
        }
    }
}

