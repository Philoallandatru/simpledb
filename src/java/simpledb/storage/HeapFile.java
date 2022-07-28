package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final TupleDesc tupleDesc;
    private File file;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.file = f;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNo = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(this.file, "r");
            if ((long) (pageNo + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException("No such page.");
            }
            byte[] bytes = new byte[BufferPool.getPageSize()]; // page you want
            f.seek((long) pageNo * BufferPool.getPageSize());
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pageNo, read));
            }
            HeapPageId heapPageId = new HeapPageId(tableId, pageNo);
            return new HeapPage(heapPageId, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

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
        return (int) (this.file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
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
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileTupleIterator(this, tid);
    }

    static class HeapFileTupleIterator implements DbFileIterator {
        private Iterator<Tuple> it;
        private HeapFile heapFile;
        private TransactionId tid;
        private int pageNo;

        HeapFileTupleIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
            this.pageNo = 0;
        }

        /**
         *
         * @param pageNo page number inside the table
         * @return
         */
        private Iterator<Tuple> getPageTupleIterator(int pageNo) throws TransactionAbortedException, DbException {
            if (pageNo >= 0 && pageNo < heapFile.numPages()) {
                /* the heapFile id is table id */
                HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(), pageNo);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(this.tid, heapPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new TransactionAbortedException();
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNo = 0;
            this.it = this.getPageTupleIterator(this.pageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.it == null) return false;
            return !((this.pageNo >= this.heapFile.numPages())
                    || (this.pageNo == this.heapFile.numPages() - 1 && !this.it.hasNext()));
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (this.it == null) {
                throw new NoSuchElementException("Iterator is null.");
            }
            /* if not reaching the last page  */
            if (this.hasNext()) {
                /* if not reaching the end of this page */
                if (this.it.hasNext()) {
                    return this.it.next();
                } else {
                    /* update page number and iterator */
                    this.pageNo += 1;
                    this.it = getPageTupleIterator(this.pageNo);
                    return it.next();
                }
            } else {
                throw new NoSuchElementException("Reach the end.");
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageNo = 0;
            this.it = null;
            this.open();
        }

        @Override
        public void close() {
            this.pageNo = 0;
            this.it = null;
        }
    }

}

