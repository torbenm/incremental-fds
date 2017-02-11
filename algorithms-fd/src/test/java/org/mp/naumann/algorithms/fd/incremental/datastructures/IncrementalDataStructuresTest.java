package org.mp.naumann.algorithms.fd.incremental.datastructures;

import org.junit.Test;
import org.mp.naumann.algorithms.fd.hyfd.PLIBuilder;
import org.mp.naumann.algorithms.fd.incremental.CompressedRecords;
import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;
import org.mp.naumann.algorithms.fd.incremental.datastructures.incremental.IncrementalDataStructureBuilder;
import org.mp.naumann.algorithms.fd.utils.PliUtils;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class IncrementalDataStructuresTest {

    @Test
    public void test() {
        PLIBuilder pliBuilder = new PLIBuilder(4, true);
        pliBuilder.addRecords(Collections.singletonList(Arrays.asList("1", "1", "1", "1")));
        List<String> columns = Arrays.asList("a", "b", "c", "d");
        DataStructureBuilder dataStructureBuilder = new IncrementalDataStructureBuilder(pliBuilder, new IncrementalFDConfiguration(""), columns);
        String schema = "";
        String tableName = "";
        List<Statement> statements = new ArrayList<>();
        Map<String, String> record = new HashMap<>();
        record.put("a", "1");
        record.put("b", "1");
        record.put("c", "2");
        record.put("d", "2");
        Statement statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        record = new HashMap<>();
        record.put("a", "1");
        record.put("b", "2");
        record.put("c", "2");
        record.put("d", "3");
        statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        record = new HashMap<>();
        record.put("a", "1");
        record.put("b", "2");
        record.put("c", "3");
        record.put("d", "4");
        statement = new DefaultInsertStatement(record, schema, tableName);
        statements.add(statement);
        Batch batch = new ListBatch(statements, schema, tableName);
        dataStructureBuilder.update(batch);
        List<? extends PositionListIndex> plis = dataStructureBuilder.getPlis();
        assertEquals(4, plis.size());
        assertEquals(1, plis.get(0).size());
        assertEquals(2, plis.get(1).size());
        assertEquals(3, plis.get(2).size());
        assertEquals(4, plis.get(3).size());
        CompressedRecords compressedRecords = dataStructureBuilder.getCompressedRecords();
        assertEquals(4, compressedRecords.size());
        // first column is the same for every record
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(0)[0]);
        assertEquals(compressedRecords.get(0)[0], compressedRecords.get(1)[0]);
        assertEquals(compressedRecords.get(0)[0], compressedRecords.get(2)[0]);
        assertEquals(compressedRecords.get(0)[0], compressedRecords.get(3)[0]);
        // second column is same for first two records and last two records
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(0)[1]);
        assertEquals(compressedRecords.get(0)[1], compressedRecords.get(1)[1]);
        assertNotEquals(compressedRecords.get(2)[1], compressedRecords.get(1)[1]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(2)[1]);
        assertEquals(compressedRecords.get(2)[1], compressedRecords.get(3)[1]);
        // third column is same for second and third recod
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(0)[2]);
        assertNotEquals(compressedRecords.get(1)[2], compressedRecords.get(0)[2]);
        assertNotEquals(compressedRecords.get(3)[2], compressedRecords.get(0)[2]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(1)[2]);
        assertEquals(compressedRecords.get(1)[2], compressedRecords.get(2)[2]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(3)[2]);
        // last column is unique for every record
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(0)[3]);
        assertNotEquals(compressedRecords.get(1)[2], compressedRecords.get(0)[2]);
        assertNotEquals(compressedRecords.get(2)[2], compressedRecords.get(0)[2]);
        assertNotEquals(compressedRecords.get(3)[2], compressedRecords.get(0)[2]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(1)[3]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(2)[3]);
        assertNotEquals(PliUtils.UNIQUE_VALUE, compressedRecords.get(3)[3]);
    }
}
