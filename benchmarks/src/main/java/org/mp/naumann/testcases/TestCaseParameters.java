package org.mp.naumann.testcases;

import org.mp.naumann.algorithms.fd.incremental.IncrementalFDConfiguration;

public class TestCaseParameters {

    final String schema, tableName, pgdb, pguser, pgpass;
    final IncrementalFDConfiguration config;
    final int stopAfter;
    final boolean hyfdOnly, hyfdCreateIndex;

    public TestCaseParameters(String schema, String tableName, IncrementalFDConfiguration config, int stopAfter,
                              boolean hyfdOnly, boolean hyfdCreateIndex, String pgdb, String pguser, String pgpass) {
        this.schema = schema;
        this.tableName = tableName;
        this.config = config;
        this.stopAfter = stopAfter;
        this.hyfdOnly = hyfdOnly;
        this.hyfdCreateIndex = hyfdCreateIndex;
        this.pgdb = pgdb;
        this.pguser = pguser;
        this.pgpass = pgpass;
    }
}
