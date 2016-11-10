package org.mp.naumann.database.jdbc.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.mp.naumann.database.statement.InsertStatement;

public class InsertStatementQueryBuilder implements StatementQueryBuilder<InsertStatement> {

    private static InsertStatementQueryBuilder instance;

    public static InsertStatementQueryBuilder get(){
        if(instance == null)
            instance = new InsertStatementQueryBuilder();
        return instance;
    }

    protected InsertStatementQueryBuilder(){

    }

    @Override
    public String generateSingle(InsertStatement statement) {
        return openStatement(statement) +
                buildKeyMap(statement) +
                buildValueMap(statement)
                + ";";
    }

    @Override
    public String generateMulti(List<InsertStatement> statements) {
        if(statements.size() > 0){
            InsertStatement base = statements.get(0);
            List<InsertStatement> equalToBase = new ArrayList<>();
            List<InsertStatement> unequalToBase = new ArrayList<>();

            for(InsertStatement stmt : statements){
                if(stmt.isOfEqualSchema(base)){
                    equalToBase.add(stmt);
                }else {
                    unequalToBase.add(stmt);
                }
            }


            String queries = unequalToBase.size() > 0 ? unequalToBase
                    .parallelStream()
                    .map(this::generateSingle)
                    .collect(Collectors.joining("\n"))+"\n" : "";

            queries += equalToBase.size() > 0 ? openStatement(base)
                    + buildKeyMap(base)
                    + equalToBase.parallelStream()
                    .map(this::buildValueMap)
                    .collect(Collectors.joining(", "))+";" : "";

            return queries;

        }
        return "";
    }


    protected String openStatement(InsertStatement statement){
        return "INSERT INTO "+statement.getTableName()+" ";
    }

    protected String buildKeyMap(InsertStatement statement){
        return "(" +
                statement
                    .getValueMap()
                    .keySet()
                    .stream().collect(Collectors.joining(", "))
                + ") VALUES ";
    }

    protected String buildValueMap(InsertStatement statement){
        return "(" +
                statement
                .getValueMap()
                .values()
                .parallelStream()
                        .map(n -> "'" + n + "'")
                .collect(Collectors.joining(", ")) +
                ")";
    }
}
