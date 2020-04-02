package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    /**
     * 传递给构造函数的默认页数。
     * 这是其他类使用的。
     * 缓冲池应该改用构造函数的numPages参数。
     *
     * */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private int maxPages;  //总页数
    private HashMap<PageId,Page> idToPages;

    private int retriveTime;
    private HashMap<PageId,Integer> idToTime;

    public BufferPool(int numPages) {
        // some code goes here
        this.maxPages=numPages;
        this.idToPages=new HashMap<>(this.maxPages);

        this.retriveTime=0;
        this.idToTime=new HashMap<>(this.maxPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    /**
     * We have not provided unit tests for BufferPool.
     * The functionality you implemented will be tested in the implementation of HeapFile below.
     * You should use the DbFile.readPage method to access pages of a DbFile.
     *
     * */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if(retriveTime>1000000)
            resetCount();
        if(!idToPages.containsKey(pid))
        {
            //throw new NoSuchElementException();

            int tableId=pid.getTableId();   // Page->Table  见定义和heapPageid.java
            DbFile file=Database.getCatalog().getDatabaseFile(tableId);
            Page page=file.readPage(pid);
            if(idToPages.size()==maxPages)
                evictPage();               //lab1未实现
            idToPages.put(pid,page);
            idToTime.put(pid,retriveTime++);
            return page;
        }

        else {
            idToTime.put(pid,retriveTime++);
            return idToPages.get(pid);
        }
    }
    private void resetCount()
    {
        List<Map.Entry<PageId,Integer>> list=new ArrayList<Map.Entry<PageId,Integer>>(idToTime.entrySet());
        list.sort(new Comparator<Map.Entry<PageId, Integer>>() {
            @Override
            public int compare(Map.Entry<PageId, Integer> o1, Map.Entry<PageId, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });
        int match=list.size();
        for(int i=0;i<match;i++)
        {
            PageId tmpId=list.get(i).getKey();
            idToTime.put(tmpId,i);
        }
        retriveTime=match;
    }//重新写idToTime里面的Time计数

    //tid--看下面！;  tid 和perm 暂时不需要

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //还没插入进去就不能调用t的gettableId()
        ArrayList<Page> tmpPage=Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid,t);
        //tmpPage.get(0).markDirty(true,tid);
        for(Page p:tmpPage) {
            p.markDirty(true,tid);
            if (idToPages.size()>maxPages)
                evictPage();               //lab1未实现
            idToPages.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> tmpPage=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid,t);   //魔鬼
        for(Page p:tmpPage) {
            p.markDirty(true, tid);
            if (idToPages.size() == maxPages)
                evictPage();               //lab1未实现
            idToPages.put(t.getRecordId().getPageId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.//
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Map<PageId,Page> map=idToPages;
        for(Map.Entry<PageId,Page> entry:map.entrySet())
        {
            if(!(entry.getValue().isDirty()==null))
            {
                flushPage(entry.getKey());
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        idToPages.remove(pid);
        idToTime.remove(pid);
    }

    /**
     * Flushes a certain page to disk   //猜测是把改完的页面写回硬盘（替换？
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page rtPg=idToPages.get(pid);

        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(rtPg);

        TransactionId tid=rtPg.isDirty();
        rtPg.markDirty(false,tid);

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2  ？？？
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        List<Map.Entry<PageId,Integer>> list=new ArrayList<Map.Entry<PageId,Integer>>(idToTime.entrySet());
        list.sort(new Comparator<Map.Entry<PageId, Integer>>() {
            @Override
            public int compare(Map.Entry<PageId, Integer> o1, Map.Entry<PageId, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        }); //从小到大排序，找到最远使用的页码

        PageId tmpId=list.get(0).getKey();

        try{
            flushPage(tmpId);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        discardPage(tmpId);

    }

}
