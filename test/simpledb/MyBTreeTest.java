package simpledb;

import org.junit.Before;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.storage.BufferPool;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.transaction.TransactionId;

public class MyBTreeTest extends SimpleDbTestBase {

    private TransactionId tid;

    @Before
    public void setUp() {
        tid = new TransactionId();
    }

    public void tearDown() {
        Database.getBufferPool().transactionComplete(tid);
        BufferPool.resetPageSize();
        Database.reset();
    }

    @Test
    public void test1() {

    }
}
