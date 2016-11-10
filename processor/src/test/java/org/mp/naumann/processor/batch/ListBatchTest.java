package org.mp.naumann.processor.batch;

import org.junit.Test;
import org.mp.naumann.database.fake.FakeDeleteStatement;
import org.mp.naumann.database.fake.FakeInsertStatement;
import org.mp.naumann.database.fake.FakeUpdateStatement;
import org.mp.naumann.database.statement.Statement;

import java.util.ArrayList;
import java.util.List;

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
        testSize(getListBatch(10, 7, 8));
        testSize(getListBatch(0,0,0));
        testSize(getListBatch(1, 0, 0));
        testSize(getListBatch(1, 0, 1));
    }

}
