package org.mp.naumann.algorithms.implementations;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mp.naumann.algorithms.result.SingleResultListener;
import org.mp.naumann.database.statement.DefaultDeleteStatement;
import org.mp.naumann.database.statement.DefaultInsertStatement;
import org.mp.naumann.database.statement.DefaultUpdateStatement;
import org.mp.naumann.database.statement.Statement;
import org.mp.naumann.processor.batch.Batch;
import org.mp.naumann.processor.batch.ListBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AverageIncrementalAlgorithmTest {

	private static final double TOLERANCE = 10e-10;
	private static final String COLUMN = "column";
	private static final String SCHEMA = "schema";
	private static final String TABLE = "table";

	@Test
	public void testInsert()  {
		SingleResultListener<Double> rl = new SingleResultListener<>();
		AverageIncrementalAlgorithm alg = new AverageIncrementalAlgorithm(COLUMN);
		alg.addResultListener(rl);
		List<Statement> statements = toInsertStatements(Arrays.asList("1", "2"));
		Batch batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(1.5, rl.getResult().doubleValue(), 10e-10);
		statements = toInsertStatements(Arrays.asList("3"));
		batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.0, rl.getResult().doubleValue(), 10e-10);
	}

	@Test
	public void testDelete()  {
		SingleResultListener<Double> rl = new SingleResultListener<>();
		AverageIncrementalAlgorithm alg = new AverageIncrementalAlgorithm(COLUMN);
		AverageDatastructure ds = new AverageDatastructure();
		ds.increaseSum(8);
		ds.increaseCount(4);
		alg.initialize(ds);
		alg.addResultListener(rl);
		List<Statement> statements = toDeleteStatements(Arrays.asList("1", "2"));
		Batch batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.5, rl.getResult().doubleValue(), 10e-10);
		statements = toDeleteStatements(Arrays.asList("3"));
		batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.0, rl.getResult().doubleValue(), 10e-10);
	}

	@Test
	public void testUpdate()  {
		SingleResultListener<Double> rl = new SingleResultListener<>();
		AverageIncrementalAlgorithm alg = new AverageIncrementalAlgorithm(COLUMN);
		AverageDatastructure ds = new AverageDatastructure();
		ds.increaseSum(8);
		ds.increaseCount(4);
		alg.initialize(ds);
		alg.addResultListener(rl);
		List<Statement> statements = toUpdateStatements(Arrays.asList(Pair.of("2", "1"), Pair.of("4", "2")));
		Batch batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.75, rl.getResult().doubleValue(), 10e-10);
		statements = toUpdateStatements(Arrays.asList(Pair.of("2", "3")));
		batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.5, rl.getResult().doubleValue(), 10e-10);
	}

	@Test
	public void testMixed()  {
		SingleResultListener<Double> rl = new SingleResultListener<>();
		AverageIncrementalAlgorithm alg = new AverageIncrementalAlgorithm(COLUMN);
		AverageDatastructure ds = new AverageDatastructure();
		ds.increaseSum(8);
		ds.increaseCount(4);
		alg.initialize(ds);
		alg.addResultListener(rl);
		List<Statement> statements = new ArrayList<>();
		statements.addAll(toUpdateStatements(Arrays.asList(Pair.of("2", "1"), Pair.of("4", "2"))));
		statements.addAll(toDeleteStatements(Arrays.asList("1", "2")));
		statements.addAll(toInsertStatements(Arrays.asList("3")));
		Batch batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(11.0 / 3.0, rl.getResult().doubleValue(), 10e-10);
		statements = new ArrayList<>();
		statements.addAll(toUpdateStatements(Arrays.asList(Pair.of("2", "3"))));
		statements.addAll(toDeleteStatements(Arrays.asList("3")));
		statements.addAll(toInsertStatements(Arrays.asList("1", "2")));
		batch = new ListBatch(statements, SCHEMA, TABLE);
		alg.handleBatch(batch);
		assertEquals(2.5, rl.getResult().doubleValue(), TOLERANCE);
	}

	private static List<Statement> toInsertStatements(List<String> values) {
		Set<String> keys = Collections.singleton(COLUMN);
		return values.stream().map(v -> new DefaultInsertStatement(Maps.asMap(keys, k -> v), SCHEMA, TABLE))
				.collect(Collectors.toList());
	}

	private static List<Statement> toDeleteStatements(List<String> values) {
		Set<String> keys = Collections.singleton(COLUMN);
		return values.stream().map(v -> new DefaultDeleteStatement(Maps.asMap(keys, k -> v), SCHEMA, TABLE))
				.collect(Collectors.toList());
	}

	private static List<Statement> toUpdateStatements(List<Pair<String, String>> values) {
		Set<String> keys = Collections.singleton(COLUMN);
		return values.stream().map(v -> new DefaultUpdateStatement(Maps.asMap(keys, k -> v.getLeft()),
				Maps.asMap(keys, k -> v.getRight()), SCHEMA, TABLE)).collect(Collectors.toList());
	}

}
