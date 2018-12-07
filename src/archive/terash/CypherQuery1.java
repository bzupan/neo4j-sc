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
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import sc.MapResult;
import sc.Neo4jCron;

public class CypherQuery1 {

    //private static final List< CypherQuery.CypherQueryObject> cypherQueryList = new ArrayList<CypherQuery.CypherQueryObject>();
    private static final ArrayList<Map<String, Object>> cypherQueryList = new ArrayList<Map<String, Object>>();
    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    //  @UserFunction
    @UserFunction
    @Description("RETURN sc.cypher.list() - list all CYPHER java calls")
    public List< Map<String, Object>> list() {
        return cypherQueryList; //.map(CronJob::new);
    }

//        //  @UserFunction
//    @Procedure()
//    @Description("RETURN sc.cypher.list() - list all CYPHER java calls")
//    public  Stream< Map<String, Object>> listProcedure() {
//        return cypherQueryList.stream(); //.map(CronJob::new);
//    }
    @UserFunction
    @Description("RETURN sc.cypher.add('name', 'cypher query', cypher query parameters') - add CYPHER java calls")
    public Map<String, Object> add(
            @Name("name") String name,
            @Name("query") String query //,
    // @Name("parameters") Map<String, Object> parameters
    ) {
//        if (parameters == null) {
//            parameters = Collections.emptyMap();
//        }

        Map<String, Object> cypherQueryMap = getObjectByKeyValue(cypherQueryList, "name", name);
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

    @UserFunction
    @Description("RETURN sc.cypher.delete('name') - add CYPHER java calls")
    public Map<String, Object> delete(
            @Name("name") String name
    ) {
        Map<String, Object> mapNode = getObjectByKeyValue(cypherQueryList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (mapNode != null) {
            log.info(mapNode.toString() + " remove " + mapNode.get("index"));
            cypherQueryList.remove(mapNode);
        }
        return null;
    }

    //  Map<String, Object> mapNode = getObjectByKeyValue(nodesList, "name", "bar5");
    private static Map<String, Object> getObjectByKeyValue(final ArrayList<Map<String, Object>> inputArray, String key, String value) {
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

//            Object arrElement = inputArray[i];
//          if (inputArray[i] ===)
        }
        return cypherNodeTmp;

    }

    @UserFunction
    @Description("RETURN sc.runCypherNode(\"stringFunctionName\", {object:\"params\"} - run node with cypher query)")
    public Object runFunction(
            @Name("name") String name,
            @Name("params") Map<String, Object> params
    ) {
        // --- find node with cypher query from node name
        String cypherFunctionQuery = "";

        log.info("runCypherNode - cypherFunctionQuery : " + name);

        Map<String, Object> cypherQueryMap = getObjectByKeyValue(cypherQueryList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            // --- run cypher query string
            cypherFunctionQuery = cypherQueryMap.get("query").toString();
            Map<String, Object> cypherResult = new HashMap<String, Object>();
            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherFunctionQuery, params);
                log.info("runCypherNode - cypher query: " + cypherFunctionQuery + ", " + params);

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
                tx2.success();
                return cypherResult;
            }
        } else {
            return null;
        }

    }

    public class CypherQueryObject { // static

        public final String name;
        public String query;
        //public Map<String, Object> parameters;

        public CypherQueryObject(String cn, String cq) {
            this.name = cn;
            this.query = cq;
            //this.parameters = {};
        }

        public CypherQueryObject(String cs) {
            this.name = cs;
            //   this.cronScheduler = new Scheduler();
        }

        public CypherQueryObject addCron() {
            return this;
        }

        public CypherQueryObject getCron() {
            return this;
        }

        public CypherQueryObject update(String cs, String cn, String cq, boolean ce) {
            //this.cronString = cs;
            //this.cronName = cn;
            //this.cypherQuery = cq;
            //this.cronEnabled = ce;

            //this.cronScheduler.stop();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof CypherQueryObject && name.equals(((CypherQueryObject) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

    }

    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("RETURN sc.cypher.runProcedure(\"stringFunctionName\", {object:\"params\"} - run node with cypher query)")
    public Stream<MapResult> runProcedure(
            @Name("name") String name,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {

        Map<String, Object> cypherQueryMap = getObjectByKeyValue(cypherQueryList, "name", name);
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

    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("RETURN sc.cypher.runProcedureDb(\"stringFunctionName\", {object:\"params\"} - run node with cypher query)")
    public Stream<MapResult> runProcedureDb(
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

//            log.info("runCypherNode - cypher query: " + cypherFunctionQuery + ", " + cypherFunctionParameters);
//         
//            // --- ger first row
//            Map<String, Object> row = dbResult.next();
//            for (Entry<String, Object> column : row.entrySet()) {
//                log.info("header"+ column.getKey());
//                log.info("data"+ column.getValue().toString());
//                List<Object> data = new ArrayList<Object>();
//                data.add(column.getValue());
//                cypherResult.put(column.getKey(), data);
//            }
//
//            // --- process rest of the rows
//            while (dbResult.hasNext()) {
//                Map<String, Object> rowNext = dbResult.next();
//                for (Entry<String, Object> column : rowNext.entrySet()) {
//                    log.info("header" + column.getKey().toString());
//                    log.info("data" + column.getValue().toString());
//                    List<Object> data = new ArrayList<Object>();
//                    data = (List<Object>) cypherResult.get(column.getKey());
//                    data.add(column.getValue());
//                    cypherResult.put(column.getKey(), data);
//                }
//            }
            tx2.success();
            return dbResult.stream().map(MapResult::new);
        }
    }

}
