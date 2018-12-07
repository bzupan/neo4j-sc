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

/**
 * RUN Neo4j Node with Cypher query
 *
 * Neo4j Cypher Node: CREATE (cqfn:CypherRunDb {cypherFunctionName:
 * "findPerson" , cypherFunctionQuery:"MATCH (n) WHERE n.name=$name", name:"",
 * age:-1}) RETURN cqfn MATCH (n:CypherRunDb) WHERE
 * n.cypherFunctionName="findPerson" RETURN n.cypherQuery
 *
 * CREATE (prs:person {name:"abc", age:22}) RETURN prs
 *
 * RETURN cypherFunctionRun (cqfn, {name:"abc", age:22})
 *
 * RETURN cypherFunctionRun (cqfn, {name:prs.name, age:prs.age})
 *
 *
 * RETURN cypherFunctionRun ("findPerson", {name:prs.name, age:prs.age})
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
/**
 *
 *
 * MATCH (n:CypherRunDb) DETACH DELETE n
 *
 * MATCH (n:CypherRunDb) RETURN n
 *
 * CREATE (cqfn:CypherRunDb) SET cqfn.cypherFunctionName=
 * "stringFunction", cqfn.cypherFunctionQuery= "Return \"string from query\""
 * RETURN cqfn
 *
 * MATCH (n:CypherRunDb) WHERE n.cypherFunctionName="stringFunction"
 * RETURN n.cypherFunctionQuery RETURN sc.runCypherNode("stringFunction")
 *
 * CREATE (cqfn:CypherRunDb {cypherFunctionName: "findPerson" ,
 * cypherFunctionQuery:"MATCH (n) WHERE n.name=\"$name\" RETURN n", name:"",
 * age:-1}) RETURN cqfn MATCH (n:CypherRunDb) WHERE
 * n.cypherFunctionName="findPerson" RETURN n.cypherFunctionQuery
 *
 * MATCH (prs:person) DELETE prs CREATE (prs:person {name:"abc", age:22}) RETURN
 * prs
 *
 * RETURN sc.runCypherNode("findPerson", {name:"abc"})
 *
 */
// MATCH (n) WHERE n.name="abc" RETURN n
// MATCH (n:CypherRunDb) WHERE n.cypherFunctionName="stringFunction" RETURN n.cypherFunctionQuery RETURN
// https://neo4j.com/docs/java-reference/3.4/javadocs/org/neo4j/graphdb/GraphDatabaseService.html
// https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/ // poglavje cypher 
// CREATE (cqfn:CypherRunDb) SET cqfn.cypherFunctionName="stringFunction", cqfn.cypherFunctionQuery= "Return \"string from query\"" RETURN cqfn
// CREATE (cqfn:CypherRunDb {cypherFunctionName: "findPerson" , cypherFunctionQuery:"MATCH (n) WHERE n.name=$name RETURN n", name:"", age:-1}) RETURN cqfn
public class RunCypherNodeFunction {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    @UserFunction
    @Description("RETURN sc.runCypherNode(\"stringFunctionName\", {object:\"params\"} - run node with cypher query)")
    public Object runCypherNodeFunction(
            @Name("cypherFunctionName") String cypherFunctionName,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {
        // --- find node with cypher query from node name
        String cypherQueryString = "MATCH (n:CypherRunDb) WHERE n.cypherFunctionName=\"" + cypherFunctionName + "\" RETURN n.cypherFunctionQuery";

        log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);

        // --- get cypher query string
        String cypherFunctionQuery = "null";
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(cypherQueryString);

            // --- some debuging
            String column = result.columns().get(0);
            Map<String, Object> row1 = result.next();
            cypherFunctionQuery = row1.get(column).toString();

            // --- debuging            
            //log.info("runCypherNode - cypherFunctionQuery : result resultAsString" + result.resultAsString());//result = resultIterator.next();
            //log.info("runCypherNode - cypherFunctionQuery : result columns" + result.columns());
            //log.info("runCypherNode - cypherFunctionQuery : result columns 0" + column);
            //log.info("runCypherNode - cypherFunctionQuery : result.next()" + row1);
//            log.info("runCypherNode - cypher query: " + cypherFunctionQuery);
//            while (result.hasNext()) {
//                Map<String, Object> row = result.next();
//                for (String key : result.columns()) {
//                    log.info("runCypherNode - cypherFunctionQuery : " + "%s = %s%n", key, row.get(key));
//                    cypherFunctionQuery = row.get(key).toString();
//                }
//            }
            tx.success();
        }

        // --- run cypher query string
       Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx2 = db.beginTx()) {
            Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
            log.info("runCypherNode - cypher query: " + cypherFunctionQuery + ", " + cypherFunctionParameters);
         
            // --- ger first row
            Map<String, Object> row = dbResult.next();
            for (Entry<String, Object> column : row.entrySet()) {
                log.info("header"+ column.getKey());
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
            tx2.success();
            return cypherResult;
         }
    }
}
