package org.mp.naumann.database.utils;

import com.opentable.db.postgres.embedded.DatabaseConnectionPreparer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresConnectionPreparer implements DatabaseConnectionPreparer {

    @Override
    public void prepare(Connection conn) throws SQLException {
        final String createSchemaSql = "CREATE SCHEMA test";
        final String createTableSql =
                "CREATE TABLE test.countries ("
                        + "country_en VARCHAR(255), "
                        + "country_de VARCHAR(255), "
                        + "country_local VARCHAR(255), "
                        + "country_code VARCHAR(255), "
                        + "continent VARCHAR(255), "
                        + "capital VARCHAR(255), "
                        + "population INTEGER, "
                        + "area INTEGER, "
                        + "coastline INTEGER, "
                        + "government_form VARCHAR(255), "
                        + "currency VARCHAR(255), "
                        + "currency_code VARCHAR(255), "
                        + "dialing_prefix VARCHAR(255), "
                        + "birthrate REAL, "
                        + "deathrate REAL, "
                        + "life_expectancy REAL, "
                        + "url VARCHAR(255));";
        final String insertSql =
                "INSERT INTO test.countries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        try (PreparedStatement stmt = conn.prepareStatement(createSchemaSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(createTableSql)) {
            stmt.execute();
        }
        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            CSVFormat format = CSVFormat.newFormat(';').withFirstRecordAsHeader().withQuote('"');
            CSVParser parser = format.parse(new FileReader(new File("../test.countries.csv")));
            for (CSVRecord csvRecord : parser) {
                for (int i = 1; i <= csvRecord.size(); i++) {
                    switch (i) {
                        case 7:
                        case 8:
                        case 9:
                            stmt.setInt(i, Integer.parseInt(csvRecord.get(i - 1)));
                            break;
                        case 14:
                        case 15:
                        case 16:
                            stmt.setFloat(i, Float.parseFloat(csvRecord.get(i - 1)));
                            break;
                        default:
                            stmt.setString(i, csvRecord.get(i - 1));
                    }
                }
                stmt.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
