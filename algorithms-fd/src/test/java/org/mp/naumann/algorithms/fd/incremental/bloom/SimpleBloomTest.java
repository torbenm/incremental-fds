package org.mp.naumann.algorithms.fd.incremental.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.PruningStrategy;
import org.mp.naumann.algorithms.fd.structures.FDTreeElementLhsPair;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleBloomTest {

    @Test
    public void testDifferentLevels() {
        List<String> columns = Arrays.asList("a", "b", "c", "d");
        SimpleBloomPruningStrategyBuilder builder1 = new SimpleBloomPruningStrategyBuilder(columns, 1);
        SimpleBloomPruningStrategyBuilder builder2 = new SimpleBloomPruningStrategyBuilder(columns, 2);
        List<Map<String, String>> records = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "1");
        record.put("b", "1");
        record.put("c", "3");
        record.put("d", "1");
        records.add(record);
        record = new HashMap<>();
        record.put("a", "1");
        record.put("b", "2");
        record.put("c", "1");
        record.put("d", "1");
        records.add(record);
        record = new HashMap<>();
        record.put("a", "0");
        record.put("b", "1");
        record.put("c", "1");
        record.put("d", "1");
        records.add(record);
        builder1.initialize(records);
        builder2.initialize(records);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        record = new HashMap<>();
        record.put("a", "0");
        record.put("b", "2");
        record.put("c", "3");
        record.put("d", "2");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        PruningStrategy strategy2 = builder2.buildStrategy(batch);
        PruningStrategy strategy1 = builder1.buildStrategy(batch);
        OpenBitSet lhs = new OpenBitSet(columns.size());
        lhs.fastSet(0);
        lhs.fastSet(1);
        lhs.fastSet(2);
        assertTrue(strategy2.cannotBeViolated(new FDTreeElementLhsPair(null, lhs)));
        assertFalse(strategy1.cannotBeViolated(new FDTreeElementLhsPair(null, lhs)));
    }

    @Test
    public void testUpdate() {
        List<String> columns = Arrays.asList("a");
        SimpleBloomPruningStrategyBuilder builder = new SimpleBloomPruningStrategyBuilder(columns, 1);
        List<Map<String, String>> records = new ArrayList<>();
        builder.initialize(records);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "0");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        PruningStrategy strategy = builder.buildStrategy(batch);
        OpenBitSet lhs = new OpenBitSet(columns.size());
        lhs.fastSet(0);
        assertTrue(strategy.cannotBeViolated(new FDTreeElementLhsPair(null, lhs)));
        strategy = builder.buildStrategy(batch);
        assertFalse(strategy.cannotBeViolated(new FDTreeElementLhsPair(null, lhs)));
    }

}
