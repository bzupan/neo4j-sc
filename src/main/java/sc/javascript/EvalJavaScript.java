package sc.javascript;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import sc.MapResult;


/**
 * This is .
 */
public class EvalJavaScript {

    private static final ArrayList<Map<String, Object>> javaScriptList = new ArrayList<Map<String, Object>>();

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.javascript.listVm() - list all JavaScript java VM calls")
    public List< Map<String, Object>> listVm() {
        return javaScriptList; 
    }

    @UserFunction
    @Description("RETURN sc.javascript.listDb() - list all JavaScript Neo4j DB  calls")
    public Map<String, Object> listDb() {
        String cypherQueryString = "MATCH (n:JavaScriptRunDb) RETURN n";

        log.info("sc.javascript.listDb(): " + cypherQueryString);
        Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            //log.info("runCypherNode - cypherFunctionQuery : " + " " + dbResult.resultAsString());

            if (dbResult.hasNext() == true) {
                // --- ger first row
                Map<String, Object> row = dbResult.next();
                for (Map.Entry<String, Object> column : row.entrySet()) {
                    log.info("header" + column.getKey());
                    log.info("data" + column.getValue().toString());
                    List<Object> data = new ArrayList<Object>();
                    data.add(column.getValue());
                    cypherResult.put(column.getKey(), data);
                }

                // --- process rest of the rows
                while (dbResult.hasNext()) {
                    Map<String, Object> rowNext = dbResult.next();
                    for (Map.Entry<String, Object> column : rowNext.entrySet()) {
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
    @Description("RETURN sc.javascript.addVm('name', 'javascript', javascript parameters') - add JavaScript Java VM calls")
    public Map<String, Object> addVm(
            @Name("name") String name,
            @Name("javaScript") String javaScript
    ) {

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(javaScriptList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            cypherQueryMap.put("javaScript", javaScript);
            //cypherQueryMap.put("parameters", name);
        } else {
            cypherQueryMap = new HashMap<String, Object>();
            log.info(cypherQueryMap.toString() + " create " + name);

            cypherQueryMap.put("name", name);
            cypherQueryMap.put("javaScript", javaScript);
            //cypherQueryMap.put("parameters", name);
            javaScriptList.add(cypherQueryMap);
        }
        return cypherQueryMap;
    }

    @Procedure(mode = Mode.WRITE) 
    @Description("RETURN sc.javascript.addDb('name', 'javascript', javascript parameters') - add JavaScript Neo4j DB calls")
    public Stream<MapResult> addDb(
            @Name("name") String name,
            @Name("javaScript") String javaScript
    ) {

        String cypherQueryString = "MERGE (n:JavaScriptRunDb {name:'" + name + "' , type:'JavaScriptRunDb'}) SET n.javaScript='" + javaScript.toString() + "' RETURN n";
        log.info("sc.javascript.addDb name / query: " + cypherQueryString);
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
    @Description("RETURN sc.javascript.deleteVm('name') - add JavaScript Java VM calls")
    public Map<String, Object> deleteVm(
            @Name("name") String name
    ) {
        Map<String, Object> mapNode = getMapFromListtByKeyValue(javaScriptList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (mapNode != null) {
            log.info(mapNode.toString() + " remove " + mapNode.get("index"));
            javaScriptList.remove(mapNode);
        }
        return null;
    }

    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("CALL sc.javascript.deleteDb('name' - delete JavaScript Neo4j DB calls")
    public Stream<MapResult> deleteDb(
            @Name("name") String name
    ) {

        String cypherQueryString = "MATCH (n:JavaScriptRunDb) WHERE n.name='" + name + "' DETACH DELETE n";
        log.info("sc.javascript.deleteDb name " + name);
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(cypherQueryString);
            tx.success();
            return dbResult.stream().map(MapResult::new);
        }
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.javascript.run(\" var properties = JSON.parse(stringObject);\\n ar replyString = 'Hello, ' + properties.name + '!';\\n return replyString;\\n \",{\"name\":\"aaa\"})")
    public Object run(
            @Name("jsScript") String jsScript,
            @Name("jsParams") Object jsParams
    ) throws ScriptException, NoSuchMethodException {
        Object javaScriptResult = null;
        log.info("sc.javascript.run : " + jsScript);
        javaScriptResult = evalJavascript(jsScript, jsParams);
        return javaScriptResult;
    }

    @UserFunction
    @Description("RETURN sc.javascript.runVm(\"stringFunctionName\", {object:\"params\"} - run JavaScript from Java VM)")
    public Object runVm(
            @Name("name") String name,
            @Name("params") Object params
    ) {

        Object javaScriptResult = null;
        Map<String, Object> javaScriptMap = getMapFromListtByKeyValue(javaScriptList, "name", name);
        String javaScriptStringVm = javaScriptMap.get("javaScript").toString();
        log.info("sc.javascript.runVm : " + name + " " + javaScriptStringVm);

        javaScriptResult = evalJavascript(javaScriptStringVm, params);
        return javaScriptResult;
    }

    @UserFunction
    @Description("RETURN sc.javascript.runCypherNode(\"stringFunctionName\", {object:\"params\"} - run JavaScript from Neo4j DB)")
    public Object runDb(
            @Name("name") String name,
            @Name("params") Object params
    ) {
        String javaScriptStringCypher = "MATCH (n:JavaScriptRunDb) WHERE n.name=\"" + name + "\" RETURN n.javaScript";
        String javaScriptStringDb = null;
        Object javaScriptResult = null;

        // --- find node with javascript from node name
        log.info("sc.javascript.runCypherNode - javaScriptStringCypher : " + javaScriptStringCypher);
        try (Transaction tx = db.beginTx()) {
            Result dbResult = db.execute(javaScriptStringCypher);
            // --- check for Neo4j Results
            if (dbResult.hasNext() == true) {
                String column = dbResult.columns().get(0);
                Map<String, Object> row1 = dbResult.next();
                javaScriptStringDb = row1.get(column).toString();
            }
            tx.success();
            javaScriptResult = evalJavascript(javaScriptStringDb, params);
        } catch (Exception ex) {
            log.error(ex.toString());
            javaScriptResult = "Get JavaScript Node Error";
        }
        return javaScriptResult;
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

//            Object arrElement = inputArray[i];
//          if (inputArray[i] ===)
        }
        return cypherNodeTmp;

    }

    private static Object evalJavascript(final String jsScript, Object jsParams) {
        Object javaScriptResult = null;
        if (jsScript != null) {
            try {
                ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

                String javaScriptStringFunction = "var javaScriptFunction = function(jsParams) {\n" + jsScript + "\n};";
                //log.info("sc.javascript.runCypherNode - javaScriptString : " + javaScriptStringFunction);

                engine.eval(javaScriptStringFunction);
                Invocable invocable = (Invocable) engine;

                javaScriptResult = invocable.invokeFunction("javaScriptFunction", jsParams);
                //log.info("sc.javascript.runCypherNode - javaScriptResult : " + javaScriptResult.toString());

            } catch (NoSuchMethodException | ScriptException ex) {
                //log.error("sc.javascript.runCypherNode - error: " + ex.toString());
                javaScriptResult = "JavaScript Run Error";
            }
        } else {
            javaScriptResult = "No JavaScript Error";
        }

        return javaScriptResult;
    }
}
