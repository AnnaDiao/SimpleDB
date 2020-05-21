package simpledb;

import java.io.IOException;
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
    int maxPages;
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private ConcurrentHashMap<PageId,Page> idToPages;
    private static int pageSize = DEFAULT_PAGE_SIZE;
    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private  ConcurrentHashMap<PageId,Integer> idToTime;
    private int retriveTime;
    private PageLockRecorder lockRecorder;


    private static class LockStru {
        TransactionId tid;
        int locktype;
        public LockStru(TransactionId tid, int locktype){
            this.tid = tid;
            this.locktype = locktype;
        }
    }
    private static class PageLockRecorder {
        ConcurrentHashMap<PageId, List<LockStru>> stateRecord;
        ConcurrentHashMap<TransactionId,PageId> waitList;

        public PageLockRecorder(){
            stateRecord = new ConcurrentHashMap<PageId, List<LockStru>>();
            waitList=new ConcurrentHashMap<>();
        }

        public synchronized boolean requireLock(PageId pid, TransactionId tid, int lockType){
            if(stateRecord.get(pid) == null){
                LockStru lockStru = new LockStru(tid, lockType);
                Vector<LockStru> lockStrus = new Vector<>();
                lockStrus.add(lockStru);
                stateRecord.put(pid, lockStrus);
                if(waitList.get(tid)!=null)
                {
                    waitList.remove(tid);
                }
                return true;
            }

            List<LockStru> lockStrus = stateRecord.get(pid);
            for(LockStru lockStru : lockStrus){
                if(lockStru.tid == tid){
                    if(lockStru.locktype == lockType)
                    {
                        if(waitList.get(tid)!=null)
                        {
                            waitList.remove(tid);
                        }
                        return true;
                    }
                    //  if(lock.locktype == 1)
                    //    return true;
                    if(lockStrus.size()==1){
                        lockStru.locktype = 1;
                        if(waitList.get(tid)!=null)
                        {
                            waitList.remove(tid);
                        }
                        return true;
                    }
                    else{
                        waitList.put(tid,pid);
                        return false;
                    }
                }
            }
            if (lockStrus.get(0).locktype ==1){
                waitList.put(tid,pid);
                return false;
            }
            if(lockType == 0){
                LockStru lockStru = new LockStru(tid, 0);
                lockStrus.add(lockStru);
                stateRecord.put(pid, lockStrus);
                if(waitList.get(tid)!=null)
                {
                    waitList.remove(tid);
                }
                return true;
            }
            waitList.put(tid,pid);
            return false;
        }
        public synchronized boolean releaseLock(PageId pid,TransactionId tid){

            List<LockStru> lockStrus = stateRecord.get(pid);
            if(lockStrus ==null)
            {
                return true;
            }
            for(int i = 0; i< lockStrus.size(); i++){
                LockStru lockStru = lockStrus.get(i);
                if(lockStru.tid == tid){
                    lockStrus.remove(lockStru);

                    if(lockStrus.size() == 0)
                        stateRecord.remove(pid);
                    return true;
                }
            }
            return false;
        }
        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            if(stateRecord.get(pid) == null)
                return false;
            List<LockStru> lockStrus = stateRecord.get(pid);
            for(LockStru lockStru : lockStrus){
                if(lockStru.tid == tid){
                    return true;
                }
            }
            return false;
        }

        private synchronized boolean waitSrc(TransactionId curHolder, List<PageId> curSrc, TransactionId tid) {
           PageId waitPg = waitList.get(curHolder);
            if (waitPg == null) {
               return false;

            }
           for (PageId tmpPid : curSrc) {
                if (tmpPid == waitPg)
                {
                    return true;    //直接等待
                }
            }
            //间接等待

            List<LockStru> holders=stateRecord.get(waitPg);
            if(holders==null||holders.size()==0)
            {
                return false;
            }

            for (LockStru pls : holders) {
               TransactionId holder = pls.tid;
               if (holder!=tid) {
                   boolean isWaiting = waitSrc(holder, curSrc, tid);
                        if (isWaiting)
                        {
                            return true;
                        }
                    }
                }
            return false;
        }

        public synchronized boolean isDeadLock(PageId pid,TransactionId tid)
        {
            //找到现在占用的Pid的资源，看他是否需要tid现在占用的资源
            List<LockStru> lcLst=stateRecord.get(pid);
            if(lcLst==null||lcLst.size()==0)
            {
                return false;
            }
            List<PageId> tidRs=new ArrayList<>();      //tid占有的资源

            for (Map.Entry<PageId, List<LockStru>> entry : stateRecord.entrySet()) {
                for (LockStru ls : entry.getValue()) {
                    if (ls.tid==tid) {
                        tidRs.add(entry.getKey());
                    }
                }
            }

            /*******************************/
            for(LockStru ls:lcLst)
            {
                TransactionId curHolder=ls.tid;
                if(curHolder!=tid) {
                    boolean isWaiting=waitSrc(curHolder,tidRs,tid);
                    if(isWaiting)
                        return true;
                }
            }

        return false;

  /*
            List<TransactionId> holdPid=new ArrayList<>();  //现在占用pid的事务tid

            List<PageId> allNeededPages=new ArrayList<>();

            for(LockStru ls:stateRecord.get(pid))
            {
                holdPid.add(ls.tid);
            }
            for(PageId tmpPid:stateRecord.keySet())
            {
                List<LockStru> tmpLs=stateRecord.get(tmpPid);
                for(LockStru lock:tmpLs)
                {
                    if(lock.tid==tid)
                    {
                        tidRs.add(tmpPid);
                    }
                }
            }
/*
            for(TransactionId t:holdPid)
            {
                if(waitList.get(t)!=null)
                {
                    if(haveCrush(tidRs,waitList.get(t)))
                        return true;
                    else
                    {
                        if(allNeededPages.indexOf(waitList.get(t))==-1)
                        {
                            allNeededPages.add(waitList.get(t));
                        }
                    }
                }
            }//直接死锁

            /******间接死锁******/
       /*     int size=allNeededPages.size();
            int pos=0;
            //System.out.println("new round");
            while (true)
            {
                while (pos<size)  //找到占用每一页的tid, 看他们是否在waitList里面等待别的页码
                {
                    System.out.println("new round");
                    PageId Tpid=allNeededPages.get(pos);

                    List<TransactionId> Ttid=new ArrayList<>();
                    for(LockStru ls:stateRecord.get(Tpid))
                    {
                        System.out.println("in R1");
                        Ttid.add(ls.tid);
                    }

                    for(TransactionId t:Ttid)
                    {
                        System.out.println("in R2");
                        if(waitList.get(t)!=null)
                        {
                            if(haveCrush(tidRs,waitList.get(t)))
                                return true;
                            else
                            {
                                System.out.println("in R3");
                                if(allNeededPages.indexOf(waitList.get(t))==-1)
                                {
                                    System.out.println("in R4");
                                    allNeededPages.add(waitList.get(t));
                                }
                            }
                        }
                    }
                    pos++;
                }
                if(size==allNeededPages.size())
                {
                    break;
                }
                size=allNeededPages.size();
                System.out.println(size);
            }
            /*
            for(PageId t:allNeededPages)
            {
                    if(haveCrush(tidRs,t))
                        return true;
            }*///间接死锁

            //return false;
        }

        private synchronized boolean haveCrush(List<PageId> tidRs, PageId pid)
        {
            for(PageId p:tidRs)
            {
                if(p==pid)
                    return true;
            }
            return false;
        }


    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param maxPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int maxPages) {
        // some code goes here
        idToPages = new ConcurrentHashMap<>();
        this.maxPages = maxPages;
        idToTime = new ConcurrentHashMap<>();
        retriveTime = 0;
        lockRecorder = new PageLockRecorder();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, IOException {
        int lockType;
        if(perm == Permissions.READ_ONLY){
            lockType = 0;
        }
        else{
            lockType = 1;
        }

        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        boolean flag= lockRecorder.requireLock(pid,tid,lockType);
        while (!flag)
        {
            /*
            if(lockRecorder.isDeadLock(pid,tid))
                throw new TransactionAbortedException();*/

            long now = System.currentTimeMillis();
            if(now-start > timeout){
                throw new TransactionAbortedException();
            }
/*
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
*/
            flag= lockRecorder.requireLock(pid,tid,lockType);
        }


        if(!idToPages.containsKey(pid)){
            int tabId = pid.getTableId();
            DbFile file = Database.getCatalog().getDatabaseFile(tabId);
            Page page = file.readPage(pid);
            if(idToPages.size()== maxPages){
                evictPage();
            }
            idToPages.put(pid,page);
            idToTime.put(pid, retriveTime++);
            return page;
        }
        return idToPages.get(pid);
        // some code goes here
    }

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
        lockRecorder.releaseLock(pid,tid);
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
        return lockRecorder.holdsLock(p,tid);
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
        if(commit){
            flushPages(tid);
        }else{
            restorePages(tid);
        }
        for(PageId pid: idToPages.keySet())
        {
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
        }

    }
    private synchronized void restorePages(TransactionId tid) throws IOException {
        for (PageId pid : idToPages.keySet()) {
            Page page = idToPages.get(pid);
            if (page.isDirty() == tid) {
                int tabId = pid.getTableId();
                DbFile file =  Database.getCatalog().getDatabaseFile(tabId);
                Page pageFromDisk = file.readPage(pid);
                idToPages.put(pid, pageFromDisk);
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
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pgLst=file.insertTuple(tid,t);
        for(Page p:pgLst){
            p.markDirty(true,tid);// update bufferpool
            if(idToPages.size() > maxPages)
                evictPage();
            idToPages.put(p.getId(),p);
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
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pgLst=file.deleteTuple(tid,t);

        for(Page p:pgLst){
            p.markDirty(true,tid);// update bufferpool
            if(idToPages.size() > maxPages)
                evictPage();
            idToPages.put(p.getId(),p);
        }
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : idToPages.keySet()) {
            flushPage(pid);
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
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page rtPg=idToPages.get(pid);

        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(rtPg);

        TransactionId tid=rtPg.isDirty();

        //rtPg.markDirty(false,tid);
        rtPg.markDirty(false,null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : idToPages.keySet()) {
            Page page = idToPages.get(pid);
            if (page.isDirty() == tid) {
                flushPage(pid);
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
        PageId pageId = null;
        int oldestAge = -1;         // find the oldest page to evict (which is not dirty)
        for (PageId pid: idToTime.keySet()) {
            Page page = idToPages.get(pid);            // skip dirty page
            if (page.isDirty() != null)
                continue;
            if (pageId == null) {
                pageId = pid;
                oldestAge = idToTime.get(pid);
                continue;
            }
            if (idToTime.get(pid) < oldestAge)
            {
                pageId = pid;
                oldestAge = idToTime.get(pid);
            }
        }
        if (pageId == null)
            throw  new DbException("failed to evict page");
        discardPage(pageId);
    }
}
