package org.mp.naumann.processor.batch;

import org.junit.Test;
import org.mockito.Mockito;
import org.mp.naumann.database.statement.DeleteStatement;
import org.mp.naumann.database.statement.InsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.database.statement.StatementVisitor;
import org.mp.naumann.database.statement.UpdateStatement;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class ListBatchTest extends BatchTest {

    ListBatch getListBatch(int inserts, int updates, int deletes) {
        List<Statement> l = new ArrayList<>();
        for (int i = 0; i < inserts; i++)
            l.add(insertMock());
        for (int i = 0; i < updates; i++)
            l.add(updateMock());
        for (int i = 0; i < deletes; i++)
            l.add(deleteMock());
        return new ListBatch(l, "", "demo");
    }

    @Test
    public void testSize() {
        checkBatchSize(getListBatch(10, 7, 8));
        checkBatchSize(getListBatch(0, 0, 0));
        checkBatchSize(getListBatch(1, 0, 0));
        checkBatchSize(getListBatch(1, 0, 1));
    }

    private InsertStatement insertMock() {
        InsertStatement mock = mock(InsertStatement.class);
        Mockito.doAnswer(a -> {
            a.getArgumentAt(0, StatementVisitor.class).visit(mock);
            return 0;
        }).when(mock).accept(any());
        return mock;
    }

    private DeleteStatement deleteMock() {
        DeleteStatement mock = mock(DeleteStatement.class);
        Mockito.doAnswer(a -> {
            a.getArgumentAt(0, StatementVisitor.class).visit(mock);
            return 0;
        }).when(mock).accept(any());
        return mock;
    }

    private UpdateStatement updateMock() {
        UpdateStatement mock = mock(UpdateStatement.class);
        Mockito.doAnswer(a -> {
            a.getArgumentAt(0, StatementVisitor.class).visit(mock);
            return 0;
        }).when(mock).accept(any());
        return mock;
    }

}
