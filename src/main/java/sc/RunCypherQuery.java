package sc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class RunCypherQuery {

    public RunCypherQuery() {

    }

    public Result executeQueryRaw(GraphDatabaseService db, String cypherQueryString) {
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            tx.success();
            return dbResult;
        }
    }

    public Result executeQueryRaw(GraphDatabaseService db, String cypherQueryString, Map<String, Object> cypherQueryParams) {
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString, cypherQueryParams);
            tx.success();
            return dbResult;
        }
    }

    public List<Map> executeQueryNode(GraphDatabaseService db, String cypherQueryString) {
        List<Map> nodes = null;
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            while (dbResult.hasNext()) {
                nodes.add((Map) dbResult.next());
            }
            tx.success();
            return nodes;
        }
    }

    public Map<String, Object> executeQueryMap(GraphDatabaseService db, String cypherQueryString) {

        Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            //log.info("runCypherNode - cypherFunctionQuery : " + " " + dbResult.resultAsString());

            if (dbResult.hasNext() == true) {
                // --- ger first row
                Map<String, Object> row = dbResult.next();
                for (Map.Entry<String, Object> column : row.entrySet()) {
//                    log.info("header" + column.getKey());
//                    log.info("data" + column.getValue().toString());
                    List<Object> data = new ArrayList<Object>();
                    data.add(column.getValue());
                    cypherResult.put(column.getKey(), data);
                }

                // --- process rest of the rows
                while (dbResult.hasNext()) {
                    Map<String, Object> rowNext = dbResult.next();
                    for (Map.Entry<String, Object> column : rowNext.entrySet()) {
//                        log.info("header" + column.getKey().toString());
//                        log.info("data" + column.getValue().toString());
                        List<Object> data = new ArrayList<Object>();
                        data = (List<Object>) cypherResult.get(column.getKey());
                        data.add(column.getValue());
                        cypherResult.put(column.getKey(), data);
                    }
                }
            }
            tx.success();
            return cypherResult;
        }
    }

}
