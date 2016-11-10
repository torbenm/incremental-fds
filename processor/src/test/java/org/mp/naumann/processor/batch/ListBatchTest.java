package org.mp.naumann.processor.batch;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mp.naumann.database.fake.FakeDeleteStatement;
import org.mp.naumann.database.fake.FakeInsertStatement;
import org.mp.naumann.database.fake.FakeUpdateStatement;
import org.mp.naumann.database.statement.Statement;

public class ListBatchTest extends BatchTest {

    ListBatch getListBatch(int inserts, int updates, int deletes){
        List<Statement> l = new ArrayList<>();
        for(int i = 0; i < inserts; i++)
            l.add(new FakeInsertStatement());
        for(int i = 0; i < updates; i++)
            l.add(new FakeUpdateStatement());
        for(int i = 0; i < deletes; i++)
            l.add(new FakeDeleteStatement());
        return new ListBatch(l,"demo");
    }

    @Test
    public void testSize(){
        checkBatchSize(getListBatch(10, 7, 8));
        checkBatchSize(getListBatch(0,0,0));
        checkBatchSize(getListBatch(1, 0, 0));
        checkBatchSize(getListBatch(1, 0, 1));
    }

}
