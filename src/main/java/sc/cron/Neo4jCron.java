package sc.cron;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;
import it.sauronsoftware.cron4j.Scheduler;
import java.io.IOException;

import sc.MapProcess;
import sc.MapResult;

import sc.VirtualNode;
import sc.Util;
import sc.RunCypherQuery;

public class Neo4jCron {

    private static final MapProcess cronMap = new MapProcess();
    private static final RunCypherQuery runCypherQuery = new RunCypherQuery();

    private static final String cronNodeLabel = "CronRunDb";
    private static final String cronDefaults = "{cronDelay:0}";

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    // ----------------------------------------------------------------------------------
    // VM add list delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("CALL sc.cron.addVm('cronName','* * * * *','MATCH (n) RETURN n', {cypherQueryParams:'optional'},{cronScheduler:'optional', cronDelay:0})   // add cron job")
    public Map<String, Object> addVm(
            @Name("name") String name,
            @Name("cronString") String cronString,
            @Name("cypherQueryString") String cypherQueryString,
            @Name(value = "cypherQueryParams", defaultValue = "{}") Map<String, Object> cypherQueryParams,
            @Name(value = "cronParams", defaultValue = cronDefaults) Map<String, Object> cronParams) {
        
        log.debug("sc.cron.addVm" + name+ " " + cronString + " " + cypherQueryString + " " + cypherQueryParams.toString() + " " + cronParams.toString() );
        Neo4jTask cronTask = new Neo4jTask(name, cypherQueryString, cypherQueryParams);
        CronScheduler cronSchedule = new CronScheduler();

        int cronDelay = Integer.parseInt((String) cronParams.get("cronDelay")); //(int) cronParams.get("cronDelay"); 


        cronSchedule.start(name, cronString, cronDelay, cronTask);

        Map<String, Object> cronObjectTmp = new HashMap<String, Object>();
        cronObjectTmp.put("name", name);
        cronObjectTmp.put("cronString", cronString);
        cronObjectTmp.put("cronDelay", cronDelay);
        cronObjectTmp.put("cronRunOk", 0);
        cronObjectTmp.put("cronRunError", 0);
        cronObjectTmp.put("cypherQuery", cypherQueryString);
        cronObjectTmp.put("cypherParams", cypherQueryParams);
        cronObjectTmp.put("cronTask", cronTask);
        cronObjectTmp.put("cronSchedule", cronSchedule);
        cronMap.addToMap(name, cronObjectTmp);

        log.info("sc.cron.add: " + cronObjectTmp.toString());
        return cronMap.getMapElementByNameClean(name);
    }

    @UserFunction
    @Description("RETURN sc.cron.listVm()  // list all cron jobs")
    public List< Map<String, Object>> listVm() {
        log.debug("sc.cron.list: " + cronMap.getListFromMapAllClean().toString());
        return cronMap.getListFromMapAllClean(); //.map(CronJob::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.listVmProc()  // list all cron jobs")
    public Stream<MapResult> listVmProc() {
        String cypherString = "MATCH (n:" + cronNodeLabel + ") RETURN n";
        return runCypherQuery.executeQueryRaw(db, cypherString).stream().map(MapResult::new);
    }

    @UserFunction
    @Description("RETURN sc.cron.deleteVm('cronName')  // remove cron job")
    public Map<String, Object> deleteVm(@Name("name") String name) {
        Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(name);

        if (!(cronObjectTmp == null)) {
            CronScheduler cronScheduler = (CronScheduler) cronObjectTmp.get("cronSchedule");

            cronScheduler.stop(name);
            cronMap.removeFromMap(name);
            log.info("sc.cron.delete: " + name + " " + cronObjectTmp.toString());
        } else {
            log.info("sc.cron.delete: not exist - " + name);
        }

        return null;
    }

    // ----------------------------------------------------------------------------------
    // start - stop
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.startCronNode('cronName')   // add cron job")
    public Stream<MapResult> startCronNode(
            @Name("name") String name) throws IOException {

        this.deleteVm(name);

        ObjectMapper mapper = new ObjectMapper();

        String cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) SET n.cronStatus='running' RETURN n";
        log.info(cypherString);
        List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");
        Map<String, Object> cypherNodeProperties = cypherNodes.get(0).getAllProperties();
        log.info((String) cypherNodeProperties.get("cronString") + (String) cypherNodeProperties.get("cypherQuery")
                + cypherNodeProperties.get("cypherParams")
                + cypherNodeProperties.get("cronParams")
                + mapper.readValue((String) cypherNodeProperties.get("cypherParams"), new TypeReference<Map<String, String>>() {
                }).toString()
                + mapper.readValue((String) cypherNodeProperties.get("cronParams"), new TypeReference<Map<String, String>>() {
                }).toString()
        );

        this.addVm(
                name,
                (String) cypherNodeProperties.get("cronString"),
                (String) cypherNodeProperties.get("cypherQuery"),
                mapper.readValue((String) cypherNodeProperties.get("cypherParams"), new TypeReference<Map<String, String>>() {
                }),
                mapper.readValue((String) cypherNodeProperties.get("cronParams"), new TypeReference<Map<String, String>>() {
                })
        );
        return Stream.of(runCypherQuery.executeQueryMap(db, cypherString)).map(MapResult::new);
        //return cypherNodes.stream().map(MapResult::new);
//        String cypherStringUpdate = "MERGE (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) SET n.cronStatus='started' RETURN n";
//        log.info(cypherString);
//        List<Node> cypherNodesUpdate = (List<Node>) runCypherQuery.executeQueryMap(db, cypherStringUpdate).get("n");
//        Map<String, Object> cypherNodeUpdate = (Map<String, Object>) cypherNodesUpdate.get(0);
//         return cypherNodeUpdate;
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.stopCronNode('cronName')   // add cron job")
    public Stream<MapResult> stopCronNode(
            @Name("name") String name) {
        String cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) SET n.cronStatus='stopped' RETURN n";
        log.info(cypherString);
        //List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");

        this.deleteVm(name);
        return  Stream.of(runCypherQuery.executeQueryMap(db, cypherString)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    // add - delete - list 
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.addCronNode('cronName','* * * * *','MERGE (n:testNode) SET n.timestamp=timestamp() RETURN n', {cypherQueryParams:'optional'},{cronScheduler:'optional', cronDelay:0})   // add cron job")
    public Stream<MapResult> addCronNode(
            @Name("name") String name,
            @Name("cronString") String cronString,
            @Name("cypherQuery") String cypherQuery,
            @Name(value = "cypherQueryParams", defaultValue = "{}") Map<String, Object> cypherQueryParams,
            @Name(value = "cronParams", defaultValue = cronDefaults) Map<String, Object> cronParams) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        // --- add to DB
        String cypherString = "MERGE (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) "
                + "SET n.cronString='" + cronString
                + "', n.cronParams='" + mapper.writeValueAsString(cronParams)
                + "', n.cypherQuery='" + cypherQuery
                + "', n.cypherParams='" + mapper.writeValueAsString(cypherQueryParams)
                + "', n.cronStatus='initialized "
                + "' RETURN n";

        log.info("sc.cron.add cypherString: " + cypherString);
        return runCypherQuery.executeQueryRaw(db, cypherString).stream().map(MapResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.deleteCronNode('cronName')   // add cron job")
    public Stream<MapResult> deleteCronNode(
            @Name("name") String name) {

        this.deleteVm(name);

        String cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) DETACH DELETE n";
        runCypherQuery.executeQueryRaw(db, cypherString);
        log.info("sc.cron.deleteCronNode cypherString: " + cypherString);

        return null;
    }

    @UserFunction
    @Description("RETURN sc.cron.listCronNodes()  // list all cron jobs")
    public Object listCronNodes() {
        // WITH   sc.cron.listDb() AS nn
        // UNWIND nn AS n Return n
        String cypherString = "MATCH (n:" + cronNodeLabel + ") RETURN n";

        List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");
        for (int i = 0; i < cypherNodes.size(); i++) {
            //cypherNodes.get(i).setProperty("aaa", i);
        }

        return cypherNodes;
        //Node aa = new Node();

//        List<String> labelNames = new ArrayList();
//        labelNames.add("CronNode"); // ['Label'];
//        Map<String, Object> props = new HashMap();
//        props.put("aa", "aa");
//
//        log.debug("sc.cron.list: " + cronMap.getListFromMapAllClean().toString());
//        //return cronMap.getListFromMapAllClean(); //.map(CronJob::new);
//        return new VirtualNode(Util.labels(labelNames), props, db);
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.runCronNode('cronName') // run cron job")
    public Stream<MapResult> runCronNode(
            @Name("name") String name
    ) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        String cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + name + "', type:'" + cronNodeLabel + "'}) RETURN n";
        log.info(cypherString);
        List<Node> cypherNodes = (List<Node>) runCypherQuery.executeQueryMap(db, cypherString).get("n");
        
        Map<String, Object> cypherNodeProperties = cypherNodes.get(0).getAllProperties();
        
//        log.info((String) cypherNodeProperties.get("cronString") + (String) cypherNodeProperties.get("cypherQuery")
//                + cypherNodeProperties.get("cypherParams")
//                + cypherNodeProperties.get("cronParams")
//                + mapper.readValue((String) cypherNodeProperties.get("cypherParams"), new TypeReference<Map<String, String>>() {
//                }).toString()
//                + mapper.readValue((String) cypherNodeProperties.get("cronParams"), new TypeReference<Map<String, String>>() {
//                }).toString()
//        );
        String cypherQuery = (String) cypherNodeProperties.get("cypherQuery");
         Map<String, Object> cypherParams = mapper.readValue((String) cypherNodeProperties.get("cypherParams"), new TypeReference<Map<String, String>>() {
                });
        
        return runCypherQuery.executeQueryRaw(db,cypherQuery , cypherParams).stream().map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    // Neo4jTask
    // ----------------------------------------------------------------------------------
    public class Neo4jTask {

        public final String taskName;
        public final String cypherQueryString;
        public final Map<String, Object> cypherQueryParams;

        public Neo4jTask(String tn, String cqs, Map<String, Object> cqp) {
            taskName = tn;
            cypherQueryString = cqs;
            cypherQueryParams = cqp;
            log.info("Neo4jTask init " + taskName);
        }

        public void run() {
            log.debug("sc.cron Neo4jTask run: " + taskName);
            Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(taskName);
            String cypherString;
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(cypherQueryString, cypherQueryParams);
                log.info("sc.cron Neo4jTask run OK - dbResult:" + dbResult.resultAsString());

                int cronRunOk = 1 + (int) cronObjectTmp.get("cronRunOk");
                cronObjectTmp.put("cronRunOk", cronRunOk);

                cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + taskName + "', type:'" + cronNodeLabel + "'}) SET n.cronRunOk=" + cronRunOk;
                tx.success();

            } catch (Exception ex) {

                int cronRunError = 1 + (int) cronObjectTmp.get("cronRunError");
                cronObjectTmp.put("cronRunError", cronRunError);

                cypherString = "MATCH (n:" + cronNodeLabel + " {name:'" + taskName + "', type:'" + cronNodeLabel + "'}) SET n.cronRunOk=" + cronRunError;
                cronObjectTmp.put("cronRunError", cronRunError);

                log.error("sc.cron Neo4jTask run ERROR: " + taskName + ex.toString());
            }
            runCypherQuery.executeQueryRaw(db, cypherString);

        }
    }

    // ----------------------------------------------------------------------------------
    // CronScheduler
    // ----------------------------------------------------------------------------------
    public class CronScheduler {

        // --- Cron scheduler 
        Scheduler cronScheduler;

        // --- Cron scheduler initialization
        public CronScheduler() {
            cronScheduler = new Scheduler();
        }

        // --- Cron scheduler start
        public Scheduler start(String cronName, String cronSchedule, int taskRunDelay, Neo4jTask cronTask) {
            cronScheduler.schedule(cronSchedule, new Runnable() {
                // --- run task
                public void run() {
                    // --- run delay
                    try {
                        Thread.sleep(taskRunDelay * 1000);
                        cronTask.run();
                    } catch (InterruptedException e) {
                        log.error("sc.cron CronScheduler error: " + cronName);
                        e.printStackTrace();
                    }
                }
            });
            cronScheduler.start();
            return cronScheduler;
        }

        // --- Cron scheduler stop
        public void stop(String cronName) {
            log.debug("sc.cron CronScheduler stop: " + cronName);
            cronScheduler.stop();
            cronScheduler.deschedule(cronScheduler.getTask(cronName));

        }

        // --- Cron scheduler stop
        public void run(String cronName, Neo4jTask cronTask) {
            log.debug("sc.cron CronScheduler run: " + cronName);
            cronTask.run();
        }
    }

}
