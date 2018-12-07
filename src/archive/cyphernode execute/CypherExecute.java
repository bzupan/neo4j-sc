package sc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class CypherExecute {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("RETURN sc.cypherExecute('') - run node with cypher query)")
    public Object cypherExecute(
            @Name("cypherFunctionName") String cypherFunctionName
    ) {
        // --- find node with cypher query from node name
        String cypherQueryString = "MATCH (n:person) WHERE n.name=\"abc\" RETURN n, n.name";

        log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);

        Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
        
            // --- ger first row
            Map<String, Object> row = dbResult.next();
            for (Entry<String, Object> column : row.entrySet()) {
                log.info("header"+ column.getKey().toString());
                log.info("data"+ column.getValue().toString());
                List<Object> data = new ArrayList<Object>();
                data.add(column.getValue());
                cypherResult.put(column.getKey(), data);
            }

            // --- process rest of the rows
            while (dbResult.hasNext()) {
                Map<String, Object> rowNext = dbResult.next();
                for (Entry<String, Object> column : rowNext.entrySet()) {
                    log.info("header" + column.getKey().toString());
                    log.info("data" + column.getValue().toString());
                    List<Object> data = new ArrayList<Object>();
                    data = (List<Object>) cypherResult.get(column.getKey());
                    data.add(column.getValue());
                    cypherResult.put(column.getKey(), data);
                }
            }
            tx.success();
            return cypherResult;
        }
    }
}
