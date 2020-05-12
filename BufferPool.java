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
    private int testNum=0;
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

    private class LockStru{
        int lotype;
        TransactionId tid;
        public LockStru(int type,TransactionId tid)
        {
            this.lotype=type;
            this.tid=tid;
        }// 0 for share 1 for exclusive
        public void changeType(int type)
        {
            this.lotype=type;
        }

    }

    /*
     * If a transaction requests a lock that it should not be granted,
     * your code should block,
     * waiting for that lock to become available
     * (i.e., be released by another transaction running in a diﬀerent thread).
     *
     * */
    private class MaintainSate{
        ConcurrentHashMap<PageId,List<simpledb.BufferPool.LockStru>> stateRec;
        public MaintainSate()
        {
            stateRec=new ConcurrentHashMap<PageId,List<simpledb.BufferPool.LockStru>>();
        }
        public synchronized boolean requestLock(PageId pid,TransactionId tid,int type)
        {
            //空的当然可以
            if(this.stateRec.get(pid)==null)
            {
                simpledb.BufferPool.LockStru tmpLk=new simpledb.BufferPool.LockStru(type,tid);
                List<simpledb.BufferPool.LockStru> tmpLst=new ArrayList<>();
                tmpLst.add(tmpLk);
                stateRec.put(pid,tmpLst);
                return true;
            }

            List<simpledb.BufferPool.LockStru> tmpLst=stateRec.get(pid);

            for(int j=0;j<tmpLst.size();j++)
            {
                simpledb.BufferPool.LockStru tmpLk=tmpLst.get(j);
                if(tid==tmpLk.tid)
                {
                    if(type==0)
                        return true;
                    if(type==1)
                    {
                        if(type==tmpLk.lotype)
                            return true;
                        if(tmpLst.size()==1)
                        {
                            tmpLk.changeType(1);
                            return true;
                        }
                        return false;
                    }
                }
            }

            if(tmpLst.get(0).lotype==1)
            {
                return false;
            }
            if(type==0)
            {
                simpledb.BufferPool.LockStru tmpLk=new simpledb.BufferPool.LockStru(type,tid);
                List<simpledb.BufferPool.LockStru> tmp=new ArrayList<>();
                tmp.add(tmpLk);
                stateRec.put(pid,tmp);
                return true;
            }
            /*if(type==1)
            {
                return false;
            }
            */
            return false;

        }

        public synchronized boolean releaseLock(PageId pid,TransactionId tid)
        {
            if(stateRec.get(pid)==null)
            {
                throw new NoSuchElementException("invalid Locked Page!");
            }
            List<simpledb.BufferPool.LockStru> LockLst=stateRec.get(pid);
            boolean find=false;
            for(int j=0;j<LockLst.size();j++)
            {
                simpledb.BufferPool.LockStru tmpLk=LockLst.get(j);
                if(tid==tmpLk.tid)
                {
                    find=true;
                    LockLst.remove(tmpLk);
                    if(LockLst.size() == 0)
                        stateRec.remove(pid);
                    return true;

                }
            }

            return false;
        }
        /** Return true if the specified transaction has a lock on the specified page */
        public synchronized boolean holdsBack(PageId pid,TransactionId tid)
        {
            if(stateRec.get(pid)==null)
            {
                return false;
            }
            List<simpledb.BufferPool.LockStru> LockLst=stateRec.get(pid);
            for(int j=0;j<LockLst.size();j++)
            {
                if(tid==LockLst.get(j).tid)
                    return true;
            }
            return false;
        }
    }



    private simpledb.BufferPool.MaintainSate mtState;


    public BufferPool(int numPages) {
        // some code goes here
        this.maxPages=numPages;
        this.idToPages=new HashMap<>(this.maxPages);

        this.retriveTime=0;
        this.idToTime=new HashMap<>(this.maxPages);
        this.mtState=new simpledb.BufferPool.MaintainSate();
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

        int type;
        if(perm==Permissions.READ_ONLY)
            type=0;
        else
        {
            type=1;
        }
        boolean flag=mtState.requestLock(pid,tid,type);
        while (!flag)
        {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            flag=mtState.requestLock(pid,tid,type);
        }

        if(retriveTime>1000000)
            resetCount();
        if(!idToPages.containsKey(pid))
        {
            //throw new NoSuchElementException();

            int tableId=pid.getTableId();   // Page->Table  见定义和heapPageid.java
            DbFile file=Database.getCatalog().getDatabaseFile(tableId);
            Page page=file.readPage(pid);
            if(idToPages.size()==maxPages)
                evictPage();
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
        mtState.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return mtState.holdsBack(p,tid);
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
        if(commit)
        {
            flushPages(tid);
        }
        else
        {
            abortPages(tid);
        }

        for(PageId pid:idToPages.keySet())
        {
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
        }
    }
    public synchronized void abortPages(TransactionId tid)
    {
        for(PageId pid:idToPages.keySet())
        {
            Page p=idToPages.get(pid);
            if (p.isDirty() != null && p.isDirty().equals(tid)) {

                Page tmpPg=Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                idToPages.put(pid,tmpPg);
            }
        }
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
            idToPages.put(p.getId(), p);
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
        for(PageId pid:idToPages.keySet())
        {
            Page p=idToPages.get(pid);
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                flushPage(pid);

                if(p.isDirty()==null)
                {
                    p.setBeforeImage();
                }
            }
        }
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
        PageId tmpId=null;
        System.out.println("call Evict!");
        System.out.println(testNum++);
        for(int j=0;j<list.size();j++)
        {
            tmpId=list.get(j).getKey();
            Page p=idToPages.get(tmpId);
            if(p.isDirty()!=null)
            {
                System.out.println(j);
                continue;
            }
            else {
                try{
                    flushPage(tmpId);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
                discardPage(tmpId);
                return;
            }

        }
        throw new DbException("Wrong in Evict! No clean page!");

    }

}
