package sc.cypher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import sc.MapProcess;
import sc.MapResult;
import sc.RunCypherQuery;

public class CypherQuery {

    private static final ArrayList<Map<String, Object>> cypherQueryList = new ArrayList<Map<String, Object>>();

    private static final MapProcess cypherQueryMap = new MapProcess();
    private static final RunCypherQuery runCypherQuery = new RunCypherQuery();

    private static final String cypherQueryNodeLabel = "CypherRunDb";

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // JAVA VM
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.cypher.addVm('name', 'MATCH (n) RETURN n') // add CYPHER Java VM calls")
    public Map<String, Object> addVm(
            @Name("name") String name,
            @Name("cypherQuery") String cypherQuery
    ) {
        // add crontask to cron map
        Map<String, Object> cypherObjectTmp = new HashMap<String, Object>();
        cypherObjectTmp.put("name", name);
        cypherObjectTmp.put("cypherQuery", cypherQuery);
        cypherQueryMap.addToMap(name, cypherObjectTmp);

        log.debug("sc.cypher.addVm: " + cypherObjectTmp.toString());
        log.info("sc.cypher.addVm: " + name);
        return cypherQueryMap.getMapElementByNameClean(name);
    }

    @UserFunction
    @Description("RETURN sc.cypher.listVm() // list all CYPHER java VM calls")
    public List< Map<String, Object>> listVm() {
        log.debug("sc.cypher.listVm: " + cypherQueryMap.getListFromMapAllClean().toString());
        return cypherQueryMap.getListFromMapAllClean(); //.map(CronJob::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.runVm('name', {object:'params'}) // run CYPHER from Java VM)")
    public Stream<MapResult> runVm(
            @Name("name") String name,
            @Name("cypherParams") Map<String, Object> cypherParams
    ) {
        Map<String, Object> cypherObjectTmp = cypherQueryMap.getMapElementByName(name);
        if (!(cypherObjectTmp == null)) {
            log.debug("sc.cypher.runVm: " + cypherObjectTmp.toString());
            log.info("sc.cypher.runVm: " + name);
            return runCypherQuery.executeQueryRaw(db, (String) cypherObjectTmp.get("cypherQuery"), cypherParams).stream().map(MapResult::new);
        } else {
            log.info("sc.cypher.runVm: not exist - " + name);
            return null;
        }
    }

    @UserFunction
    @Description("RETURN sc.cypher.deleteVm('name') - add CYPHER Java VM calls")
    public Map<String, Object> deleteVm(
            @Name("name") String name
    ) {
        Map<String, Object> cypherObjectTmp = cypherQueryMap.getMapElementByName(name);
        if (!(cypherObjectTmp == null)) {
            log.debug("sc.cypher.deleteVm: " + cypherObjectTmp.toString());
            cypherQueryMap.removeFromMap(name);
            log.info("sc.cypher.deleteVm: " + name);
        } else {
            log.info("sc.cypher.deleteVm: not exist - " + name);
        }
        return null;
    }

    // ----------------------------------------------------------------------------------
    // Neo4j DB
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("CALL sc.cypher.addDb('cypherRunDb', 'MATCH (n) RETURN n')  //add CYPHER Neo4j DB calls")
    public Stream<MapResult> addDb(
            @Name("name") String name,
            @Name("cypherQuery") String cypherQuery
    ) {
        // --- add to DB
        String cypherQueryModified = cypherQuery.replaceAll("'", "\\\\'");
        String cypherString = "MERGE (n:" + cypherQueryNodeLabel + " {name:'" + name + "', type:'" + cypherQueryNodeLabel + "'}) "
                + "SET n.cypherQuery='" + cypherQueryModified + "' RETURN n";

        log.info("sc.cypher.addDb cypherString: " + cypherQueryModified);
        log.info("sc.cypher.addDb: " + name);
        return runCypherQuery.executeQueryRaw(db, cypherString).stream().map(MapResult::new);
    }

    @UserFunction
    @Description("RETURN sc.cypher.listDb() - list all CYPHER Neo4j DB calls")
    public Object listDb() {
        // WITH   sc.cron.listDb() AS nn
        // UNWIND nn AS n Return n
        String cypherString = "MATCH (n:" + cypherQueryNodeLabel + ") RETURN n";
        log.debug("sc.cypher.listDb cypherString: " + cypherString);

        List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");
        for (int i = 0; i < cypherNodes.size(); i++) {
            //cypherNodes.get(i).setProperty("aaa", i);
        }

        return cypherNodes;
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.runDb('cypherRunDb', {object:'params'}) // rrun CYPHER from Neo4j DB)")
    public Stream<MapResult> runDb(
            @Name("name") String name,
            @Name(value = "cypherParams", defaultValue = "{}") Map<String, Object> cypherParams
    ) {

        // --- get node
        String cypherString = "MATCH (n:" + cypherQueryNodeLabel + " {name:'" + name + "', type:'" + cypherQueryNodeLabel + "'}) RETURN n";
        log.debug("sc.cypher.runDb cypherString get cron node: " + cypherString);
        List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");

        // --- get query
        Map<String, Object> cypherNodeProperties = cypherNodes.get(0).getAllProperties();
        String cypherQuery = (String) cypherNodeProperties.get("cypherQuery");

        log.debug("sc.cypher.runDb cypherString to run: " + cypherString + cypherParams.toString());
        log.info("sc.cypher.runDb: " + name);
        return runCypherQuery.executeQueryRaw(db, cypherQuery, cypherParams).stream().map(MapResult::new);
    }

    @Procedure(mode = Mode.WRITE) //name = "sc.runCypherNodeProcedure",
    @Description("CALL sc.cypher.deleteDb('cypherRunDb') - add CYPHER Neo4j DB calls")
    public Stream<MapResult> deleteDb(
            @Name("name") String name
    ) {
        // --- remove node
        String cypherString = "MATCH (n:" + cypherQueryNodeLabel + " {name:'" + name + "', type:'" + cypherQueryNodeLabel + "'}) DETACH DELETE n";
        log.debug("sc.cypher.deleteDb cypherString: " + cypherString);

        runCypherQuery.executeQueryRaw(db, cypherString);
        log.info("sc.cypher.deleteDb: " + name);
        return null;
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.run(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Stream<MapResult> run(
            @Name("cypherQuery") String cypherQuery,
            @Name("cypherParams") Map<String, Object> cypherParams
    ) {
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQuery != null) {
            log.info("runCypherNode - cypherFunctionQuery: " + cypherQuery);
            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherQuery, cypherParams);
                tx2.success();
                return dbResult.stream().map(MapResult::new);
            }
        } else {
            return null;
        }
    }

}
