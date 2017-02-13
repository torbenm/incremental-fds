package org.mp.naumann.algorithms.fd.incremental.pruning.bloom;

import org.apache.lucene.util.OpenBitSet;
import org.junit.Test;
import org.mp.naumann.algorithms.fd.incremental.pruning.ValidationPruner;
import org.mp.naumann.algorithms.fd.structures.LatticeElementLhsPair;
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
        BloomPruningStrategy builder1 = new BloomPruningStrategy(columns).addGenerator(new AllCombinationsBloomGenerator(1));
        BloomPruningStrategy builder2 = new BloomPruningStrategy(columns).addGenerator(new AllCombinationsBloomGenerator(2));
        List<String[]> records = new ArrayList<>();
        records.add(new String[] {"1", "1", "3", "1"});
        records.add(new String[] {"1", "2", "1", "1"});
        records.add(new String[] {"0", "1", "1", "1"});
        builder1.initialize(records);
        builder2.initialize(records);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "0");
        record.put("b", "2");
        record.put("c", "3");
        record.put("d", "2");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        ValidationPruner strategy2 = builder2.analyzeBatch(batch);
        ValidationPruner strategy1 = builder1.analyzeBatch(batch);
        OpenBitSet lhs = new OpenBitSet(columns.size());
        lhs.fastSet(0);
        lhs.fastSet(1);
        lhs.fastSet(2);
        assertTrue(strategy2.doesNotNeedValidation(new LatticeElementLhsPair(lhs, null)));
        assertFalse(strategy1.doesNotNeedValidation(new LatticeElementLhsPair(lhs, null)));
    }

    @Test
    public void testUpdate() {
        List<String> columns = Arrays.asList("a");
        BloomPruningStrategy builder = new BloomPruningStrategy(columns).addGenerator(new AllCombinationsBloomGenerator(1));
        List<String[]> records = new ArrayList<>();
        builder.initialize(records);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "0");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        ValidationPruner strategy = builder.analyzeBatch(batch);
        OpenBitSet lhs = new OpenBitSet(columns.size());
        lhs.fastSet(0);
        assertTrue(strategy.doesNotNeedValidation(new LatticeElementLhsPair(lhs, null)));
        strategy = builder.analyzeBatch(batch);
        assertFalse(strategy.doesNotNeedValidation(new LatticeElementLhsPair(lhs, null)));
    }

    @Test
    public void testInnerCombination() {
        List<String> columns = Arrays.asList("a");
        BloomPruningStrategy builder = new BloomPruningStrategy(columns).addGenerator(new AllCombinationsBloomGenerator(1));
        List<String[]> records = new ArrayList<>();
        builder.initialize(records);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "0");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        ValidationPruner strategy = builder.analyzeBatch(batch);
        OpenBitSet lhs = new OpenBitSet(columns.size());
        lhs.fastSet(0);
        assertFalse(strategy.doesNotNeedValidation(new LatticeElementLhsPair(lhs, null)));
    }

}
