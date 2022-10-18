package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    enum LockType {
        SLOCK, XLOCK
    }
    static class PageLock {
        LockType lockType;
        PageId pageId;
        private final Set<TransactionId> txns;

        PageLock(LockType type, TransactionId tid, PageId pageId) {
            lockType = type;
            this.pageId = pageId;
            txns = new HashSet<>();
            txns.add(tid);
        }

        public PageId getPageId() { return pageId; }

    }
    static class LockManager {
        public final ConcurrentMap<PageId, PageLock> pageLocks;
        public final ConcurrentMap<TransactionId, Set<PageId>> pagesLockedByTid;

        public ConcurrentMap<TransactionId, Set<PageId>> getPagesLockedByTid() {
            return pagesLockedByTid;
        }

        public LockManager() {
            pageLocks = new ConcurrentHashMap<>();
            pagesLockedByTid = new ConcurrentHashMap<>();
        }

        private void checkRep() {
            Set<PageId> pageIds = pageLocks.keySet();
            for (PageId pageId : pageIds) {
                Set<TransactionId> holdByTxn = pageLocks.get(pageId).txns;
                for (TransactionId tid : holdByTxn) {
                    if (!pagesLockedByTid.containsKey(tid)) {
                        throw new RuntimeException("Should have tid in pagesLockedById");
                    }
                    if (!pagesLockedByTid.get(tid).contains(pageId)) {

                        throw new RuntimeException("Should have pageId " + pageId +  " in txn " + tid + " pagesLockedById");
                    }
                }
            }
        }


        public synchronized boolean acquiresLock(TransactionId tid , PageId pageId, LockType lockType) throws TransactionAbortedException {
            // checkRep();
            // if there is no such lock, create a lock with given type
            boolean log = false;
            String threadName = Thread.currentThread().getName();
            String baseInfo =  "------ACQUIRE------[Tid " + tid.getId() + " % " + threadName + " % " + lockType + " % " + pageId.getPageNumber() + "]";
            if (log) System.out.println(baseInfo + " ----START.");

            if (!pageLocks.containsKey(pageId)) {
                if (log) System.out.println(baseInfo + ": no lock. add lock.----END.");
                pageLocks.put(pageId, new PageLock(lockType, tid, pageId));

                if (!pagesLockedByTid.containsKey(tid)) {
                    Set<PageId> tidPages = new HashSet<>();
                    tidPages.add(pageId);
                    pagesLockedByTid.put(tid, tidPages);
                }
                pagesLockedByTid.get(tid).add(pageId);
                return true;
            }

            PageLock pageLock = pageLocks.get(pageId);
            LockType oldLockType = pageLock.lockType;
            Set<TransactionId> ownerTxns = pageLock.txns;

            // if there is shared lock, and acquires for a shared lock, granted
            if (oldLockType == LockType.SLOCK
                    && lockType == LockType.SLOCK && !ownerTxns.contains(tid)) {
                if (log) System.out.println(baseInfo + "]: already has a shared lock, granted.----END");
                pageLock.txns.add(tid);
                if (!pagesLockedByTid.containsKey(tid)) {
                    Set<PageId> tidPages = new HashSet<>();
                    tidPages.add(pageId);
                    pagesLockedByTid.put(tid, tidPages);
                } else {
                    pagesLockedByTid.get(tid).add(pageId);
                }
                return true;
            }

            if (ownerTxns.contains(tid)) {
                if (lockType == oldLockType) {
                    if (log) System.out.println(baseInfo + ": already has the same shared or exclusive lock.-----END");
                    return true;
                } else if (oldLockType == LockType.SLOCK && lockType == LockType.XLOCK) {
                    // check if it can update the lock
                    if ( ownerTxns.size() == 1) {
                        if (log) System.out.println(baseInfo + ": want to update lock, hold by it self, update.---END");
                        pageLock.lockType = LockType.XLOCK;
                        return true;
                    } else {
                        if (log) System.out.println(baseInfo + ": want to update lock, hold by other, cannot update. ----WAIT");
                        throw new TransactionAbortedException();
                    }
                } else if (oldLockType == LockType.XLOCK && lockType == LockType.SLOCK) {
                    if (log) System.out.println(baseInfo + ": want to do read on write, granted.----END");
                    return true;
                } else {
                    if (log) System.out.println("Error.");
                }

            }

            if (log) System.out.println(baseInfo + ": wait..----WAIT");

            try {
                wait(20);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return false;
        }


        public synchronized void releaseLock(TransactionId tid ,PageId pageId) {
            boolean log = false;
            String threadName = Thread.currentThread().getName();
            String baseInfo = "+++++RELEASE++++++[Tid:" + tid.getId() + " % " + threadName + " % "  + pageId.getPageNumber() + "]";
            if (log) System.out.println("START: " + baseInfo + ": releasing...");
            // check if this txn has the look it wanna release?
            if (!pageLocks.containsKey(pageId)) {
                if (log) System.out.println("ERROR" + baseInfo + ": pageLocks keySet problem.");
                return;
            }
            if (!pagesLockedByTid.containsKey(tid)) {
                if (log) System.out.println("ERROR " + baseInfo +  " pagesLockedByTid keySet problem.");
                return;
            }
            PageLock pageLock = pageLocks.get(pageId);
            Set<TransactionId> ownerTxns = pageLock.txns;
            // cannot release lock hold by other txns
            if (!ownerTxns.contains(tid)) {
                if (log) System.out.println("ERROR " + baseInfo + ": pageLocks values problem");
                return;
            }

            if (!pagesLockedByTid.get(tid).contains(pageId)) {
                if (log) System.out.println("ERROR " + baseInfo + ": pagesLockedByTid values problem");
                return;
            }

            LockType type = pageLock.lockType;
            // if it is shared lock, remove this lock from list,
            // if list is empty, remove the lock from the hashtable
            if (type == LockType.SLOCK) {
                pageLock.txns.remove(tid);
                pagesLockedByTid.get(tid).remove(pageId);
                if (ownerTxns.isEmpty()) {
                    pageLocks.remove(pageId);
                    if (pagesLockedByTid.get(tid).isEmpty() ) {
                        pagesLockedByTid.remove(tid);
                    }
                }
            } else {
                // it is exclusive lock
                pageLocks.remove(pageId);
                pagesLockedByTid.get(tid).remove(pageId);
                if (pagesLockedByTid.get(tid).isEmpty() ) {
                    pagesLockedByTid.remove(tid);
                }
            }
            notifyAll();
        }
    }

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /** map page_id to pages, if
     */
    private final Deque<PageId> doublyDeque;

    /* buffered pages */
    private final ConcurrentMap<PageId, Page> pages;
    private final int numPages;
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages    maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.doublyDeque = new LinkedList<>();
        lockManager = new LockManager();
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        // check permission
        LockType lockType;
        if (perm == Permissions.READ_ONLY) lockType = LockType.SLOCK;
        else lockType = LockType.XLOCK;

        long st = System.currentTimeMillis();
        boolean isAcquired = false;
        while (!isAcquired) {
            isAcquired = lockManager.acquiresLock(tid, pid, lockType);
            if (System.currentTimeMillis() - st > 1000)
                throw new TransactionAbortedException();
        }

        if (this.pages.containsKey(pid)) {
            doublyDeque.remove(pid);
            doublyDeque.push(pid);
            return pages.get(pid);
        } else {
            /* this page not in buffer, get it from database */
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);

            /* if this page not in that file return null */
            if (page == null) return null;

            if (pages.size() >= numPages) {
                evictPage();
            }
            pages.put(pid, page);
            doublyDeque.push(pid);
            return page;
        }
//        throw new TransactionAbortedException();
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockManager.pageLocks.containsKey(p)) return false;
        if (!lockManager.pageLocks.get(p).txns.contains(tid)) return false;
        return true;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockManager.getPagesLockedByTid().containsKey(tid)) return;
        if (commit) {

            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            restorePages(tid);
        }
        // release lock
        // copy initialization
        Set<PageId>  pageIds = new HashSet<>(lockManager.getPagesLockedByTid().get(tid));
        for (PageId pageId : pageIds)
            lockManager.releaseLock(tid, pageId);
    }

    public void restorePages(TransactionId tid) {
        // get dirty pageIds
        // read pages from disk
        // replace them in file
        Set<PageId> pageIds = lockManager.getPagesLockedByTid().get(tid);
        List<PageId> pageIdList = pageIds.stream().toList();
        int tableId = pageIdList.get(0).getTableId();
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        for (PageId pageId : pageIds) {
            if (pageId.getTableId() != tableId) {
                tableId = pageId.getTableId();
                tableFile = Database.getCatalog().getDatabaseFile(tableId);
            }
            Page page = tableFile.readPage(pageId);
            pages.remove(pageId);
            pages.put(pageId, page);

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
        /* find the table */
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        /* insert the tuple */
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            pages.put(page.getId(), page);
            doublyDeque.push(page.getId());
        }
        // not necessary for lab1

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
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            pages.put(page.getId(), page);
        }

        // not necessary for lab1
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Set<PageId> bufferedPages = pages.keySet();
        for (PageId pageId : bufferedPages) {
            if (pages.get(pageId).isDirty() != null) {
                flushPage(pageId);
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

        /*
        if (lockManager.pageLocks.containsKey(pid)) {
            Set<TransactionId> tids = lockManager.pageLocks.get(pid).txns;
            for (TransactionId tid : tids) {
                if (lockManager.pagesLockedByTid.containsKey(tid)) {
                    lockManager.pagesLockedByTid.get(tid).remove(pid);
                    if (lockManager.pagesLockedByTid.get(pid).isEmpty()) {
                        lockManager.pagesLockedByTid.remove(tid);
                    }
                }
            }
            lockManager.pageLocks.remove(pid);
        }
         */
        pages.remove(pid);
        doublyDeque.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        int tableId = pid.getTableId();
        DbFile tableFile = Database.getCatalog().getDatabaseFile(tableId);
        tableFile.writePage(pages.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (!lockManager.getPagesLockedByTid().containsKey(tid)) return;
        Set<PageId> pageIds = new HashSet<>(lockManager.getPagesLockedByTid().get(tid));
        for (PageId pageId : pageIds) {
            if (pages.containsKey(pageId)) {
                flushPage(pageId);
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
        // PageId lastPage = doublyDeque.removeLast();
        Iterator<PageId> it = doublyDeque.descendingIterator();
        PageId notDirtyPageId = null;
        while (it.hasNext()) {
            PageId pageId = it.next();
            if (pages.get(pageId).isDirty() == null) {
                notDirtyPageId = pageId;
                break;
            }
        }
        /*
        Set<PageId>  pageIds = pages.keySet();
        for (PageId pageId : pageIds) {
            if (pages.get(pageId).isDirty() == null) {
                notDirtyPageId =
            }
        }
         */

        if (notDirtyPageId != null) {
            /* flush this page to disk */
            try {
                flushPage(notDirtyPageId);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush page to dish.");
            }
            discardPage(notDirtyPageId);
        } else {
            throw new DbException("All pages are dirty, cannot do eviction.");
        }




    }

}
