package sc.cypher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import sc.MapResult;


public class CypherQuery {

    private static final ArrayList<Map<String, Object>> cypherQueryList = new ArrayList<Map<String, Object>>();
    
    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.cypher.listVm() - list all CYPHER java VM calls")
    public List< Map<String, Object>> listVm() {
        return cypherQueryList; //.map(CronJob::new);
    }

    @UserFunction
    @Description("RETURN sc.cypher.listDb() - list all CYPHER Neo4j DB calls")
    public Map<String, Object> listDb() {
        String cypherQueryString = "MATCH (n:CypherRunDb) RETURN n";

        log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);
        Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            //log.info("runCypherNode - cypherFunctionQuery : " + " " + dbResult.resultAsString());

            if (dbResult.hasNext() == true) {
                // --- ger first row
                Map<String, Object> row = dbResult.next();
                for (Entry<String, Object> column : row.entrySet()) {
                    log.info("header" + column.getKey());
                    log.info("data" + column.getValue().toString());
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
            }
            tx.success();
            return cypherResult;
        }
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.cypher.addVm('name', 'cypher query', cypher query parameters') - add CYPHER Java VM calls")
    public Map<String, Object> addVm(
            @Name("name") String name,
            @Name("query") String query
    ) {
        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherQueryList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            cypherQueryMap.put("query", query);
            //cypherQueryMap.put("parameters", name);
        } else {
            cypherQueryMap = new HashMap<String, Object>();
            log.info(cypherQueryMap.toString() + " create " + name);

            cypherQueryMap.put("name", name);
            cypherQueryMap.put("query", query);
            //cypherQueryMap.put("parameters", name);
            cypherQueryList.add(cypherQueryMap);
        }
        return cypherQueryMap;
    }

    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("RETURN sc.cypher.addDb('name', 'cypher query', cypher query parameters') - add CYPHER Neo4j DB calls")
    public Stream<MapResult> addDb(
            @Name("name") String name,
            @Name("query") String query
    ) {

        String cypherQueryString = "MERGE (n:CypherRunDb {name:'" + name + "' , type:'CypherRunDb'}) SET n.query='" + query + "' RETURN n";

        log.info("sc.cypher.addDb name / query: " + cypherQueryString);
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            tx.success();
            return dbResult.stream().map(MapResult::new);
        }
    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.cypher.deleteVm('name') - add CYPHER Java VM calls")
    public Map<String, Object> deleteVm(
            @Name("name") String name
    ) {
        Map<String, Object> mapNode = getMapFromListtByKeyValue(cypherQueryList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (mapNode != null) {
            log.info(mapNode.toString() + " remove " + mapNode.get("index"));
            cypherQueryList.remove(mapNode);
        }
        return null;
    }

        @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("RETURN sc.cypher.deleteDb('name', 'cypher query', cypher query parameters') - add CYPHER Neo4j DB calls")
    public Stream<MapResult> deleteDb(
            @Name("name") String name
    ) {
        String cypherQueryString = "MATCH (n:CypherRunDb) WHERE n.name='" + name + "' DETACH DELETE n";
        log.info("sc.cypher.deleteDb name / query: " + cypherQueryString);
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            tx.success();
            return dbResult.stream().map(MapResult::new);
        }
    }
    
    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("RETURN sc.cypher.runProcedureVm(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Stream<MapResult> run(
            @Name("cypherQueryString") String cypherQueryString,
            @Name("cypherParamsObject") Map<String, Object> cypherParamsObject
    ) {
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryString != null) {
            log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);
            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherQueryString, cypherParamsObject);
                tx2.success();
                return dbResult.stream().map(MapResult::new);
            }
        } else {
            return null;
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.runVm(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Stream<MapResult> runVm(
            @Name("name") String name,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherQueryList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            // --- run cypher query string
            String cypherFunctionQuery = cypherQueryMap.get("query").toString();

            log.info("runCypherNode - cypherFunctionQuery : " + cypherFunctionQuery);

            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
                tx2.success();
                return dbResult.stream().map(MapResult::new);
            }

        } else {
            return null;
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("RETURN sc.cypher.runProcedureDb(\"stringFunctionName\", {object:\"params\"} - rrun CYPHER from Neo4j DB)")
    public Stream<MapResult> runDb(
            @Name("cypherFunctionName") String cypherFunctionName,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {
        // --- find node with cypher query from node name
        String cypherQueryString = "MATCH (n:CypherRunDb) WHERE n.name=\"" + cypherFunctionName + "\" RETURN n.query";

        log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);

        // --- get cypher query string
        String cypherFunctionQuery = "null";
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(cypherQueryString);

            // --- some debuging
            String column = result.columns().get(0);
            Map<String, Object> row1 = result.next();
            cypherFunctionQuery = row1.get(column).toString();
            tx.success();
        }

        // --- run cypher query string
        try (Transaction tx2 = db.beginTx()) {
            Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
            tx2.success();
            return dbResult.stream().map(MapResult::new);
        }
    }

    // ----------------------------------------------------------------------------------
    // util
    // ----------------------------------------------------------------------------------
    private static Map<String, Object> getMapFromListtByKeyValue(final ArrayList<Map<String, Object>> inputArray, String key, String value) {
        Map<String, Object> cypherNodeTmp = null; //new HashMap<String, Object>();

        for (int i = 0; i < inputArray.size(); ++i) {
            System.out.println(key + " " + value);
            Map<String, Object> arrElement = inputArray.get(i);
            if (arrElement != null) {
                if (arrElement.get(key) != null) {
                    String currentKeyValue = arrElement.get(key).toString();
                    //   arrElement
                    //System.out.println(key + i);
                    //System.out.println(inputArray.get(i).toString());
                    System.out.println(" - " + currentKeyValue + " " + value);
                    if (currentKeyValue.equals(value)) {
                        System.out.println(" - -----------------------");
                        cypherNodeTmp = arrElement;
                        //cypherNodeTmp = new HashMap<String, Object>(arrElement);
                        //cypherNodeTmp.put("index", i);
                    } else {
                        System.out.println(" - -");
                    }
                }
            }
        }
        return cypherNodeTmp;
    }
}
