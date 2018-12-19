package sc.cron;

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

import sc.MapProcess;
import sc.MapResult;

import sc.VirtualNode;
import sc.Util;


public class Neo4jCron {

    private static final MapProcess cronMap = new MapProcess();
    private static final String cronDefaults = "{cronDelay:0}";

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    //  @UserFunction
    @UserFunction
    @Description("RETURN sc.cron.list()  // list all cron jobs")
    public List< Map<String, Object>> list() {
        log.debug("sc.cron.list: " + cronMap.getListFromMapAllClean().toString());
        return cronMap.getListFromMapAllClean(); //.map(CronJob::new);
    }

    @UserFunction
    @Description("RETURN sc.cron.listVnode()  // list all cron jobs")
    public Node listVnode() {

        List<String> labelNames = new ArrayList();
        labelNames.add("CronNode"); // ['Label'];
        Map<String, Object> props = new HashMap();
        props.put("aa", "aa");

       
        log.debug("sc.cron.list: " + cronMap.getListFromMapAllClean().toString());
        //return cronMap.getListFromMapAllClean(); //.map(CronJob::new);
        return new VirtualNode(Util.labels(labelNames), props, db);
        //return null;
    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.cron.delete('cronName')  // remove cron job")
    public Map<String, Object> delete(@Name("name") String name) {
        Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(name);
        CronScheduler cronScheduler = (CronScheduler) cronObjectTmp.get("cronSchedule");
        cronScheduler.stop(name);
        cronMap.removeFromMap(name);

        log.info("sc.cron.delete: " + cronObjectTmp.toString());
        return null;
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron.add('cronName','* * * * *','MATCH (n) RETURN n', {cypherQueryParams:'optional'},{cronScheduler':optional', cronDelay:0})   // add cron job")
    public Stream<MapResult> add(
            @Name("name") String name,
            @Name("cron") String cron,
            @Name("cypherQueryString") String cypherQueryString,
            @Name(value = "cypherQueryParams", defaultValue = "{}") Map<String, Object> cypherQueryParams,
            @Name(value = "cronParams", defaultValue = cronDefaults) Map<String, Object> cronParams) {

        Neo4jTask cronTask = new Neo4jTask(name, cypherQueryString, cypherQueryParams);
        CronScheduler cronSchedule = new CronScheduler();
        Long cronDelay = (Long) cronParams.get("cronDelay");
        cronSchedule.start(name, cron, cronDelay, cronTask);

        Map<String, Object> cronObjectTmp = new HashMap<String, Object>();
        cronObjectTmp.put("name", name);
        cronObjectTmp.put("cronString", cron);
        cronObjectTmp.put("cronDelay", cronDelay);
        cronObjectTmp.put("cronRunOk", 0);
        cronObjectTmp.put("cronRunError", 0);
        cronObjectTmp.put("cypherQuery", cypherQueryString);
        cronObjectTmp.put("cypherParams", cypherQueryParams);
        cronObjectTmp.put("cronTask", cronTask);
        cronObjectTmp.put("cronSchedule", cronSchedule);
        cronMap.addToMap(name, cronObjectTmp);

        log.info("sc.cron.add: " + cronObjectTmp.toString());
        return Stream.of(cronMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure()
    @Description("CALL sc.cron.run('cronName') // run cron job")
    public Stream<MapResult> run(
            @Name("name") String name
    ) {
        Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(name);
        CronScheduler cronScheduler = (CronScheduler) cronObjectTmp.get("cronSchedule");
        Neo4jTask cronTask = (Neo4jTask) cronObjectTmp.get("cronTask");
        cronScheduler.run(name, cronTask);

        log.info("sc.cron.run: " + cronObjectTmp.toString());
        return null;
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
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(cypherQueryString, cypherQueryParams);
                log.info("sc.cron Neo4jTask run OK - dbResult:" + dbResult.resultAsString());

                cronObjectTmp.put("cronRunOk", 1 + (int) cronObjectTmp.get("cronRunOk"));
                tx.success();
            } catch (Exception ex) {
                cronObjectTmp.put("cronRunError", 1 + (int) cronObjectTmp.get("cronRunError"));
                //cronObjectTmp.put("cronRunErrorMessage", "Task run error" + taskName + ex.toString());
                log.error("sc.cron Neo4jTask run ERROR: " + taskName + ex.toString());
            }
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
        public Scheduler start(String cronName, String cronSchedule, Long taskRunDelay, Neo4jTask cronTask) {
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
