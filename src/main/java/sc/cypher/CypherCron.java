package sc.cypher;

// http://www.sauronsoftware.it/projects/cron4j/manual.php
// https://mvnrepository.com/artifact/it.sauronsoftware.cron4j/cron4j/2.2.5
// https://github.com/Takuto88/cron4j
//import apoc.Pools;
//import apoc.util.Util;
import it.sauronsoftware.cron4j.Scheduler;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

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

//import static apoc.util.Util.merge;
import static java.lang.System.nanoTime;
import static java.util.Collections.singletonMap;

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskTable;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import sc.MapProcess;
import sc.MapResult;

public class CypherCron {

    private static final MapProcess cronMap = new MapProcess();
    private static final String cronDefaults = "{}";

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
    @Description("return sc.cypher.listCron() - list all jobs")
    public List< Map<String, Object>> listCron() {
        return cronMap.getListFromMapAllClean(); //.map(CronJob::new);
    }

    // ----------------------------------------------------------------------------------
    // deleteCron
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("call sc.cypher.deleteCron('cronName') - list all jobs")
    public Map<String, Object> deleteCron(@Name("name") String name) {
        Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(name);
        CronScheduler cronScheduler = (CronScheduler) cronObjectTmp.get("cronSchedule");
        cronScheduler.stop(name);
        cronMap.removeFromMap(name);
        return null;
    }

    // ----------------------------------------------------------------------------------
    // addCron
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.addCron('cronName','* * * * *','MATCH (n) RETURN n', {})   submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
    public Stream<MapResult> addCron(
            @Name("name") String name,
            @Name("cron") String cron,
            @Name("cypherQueryString") String cypherQueryString,
            @Name(value = "cypherQueryParams", defaultValue = "{}") Map<String, Object> cypherQueryParams,
            @Name(value = "cronParams", defaultValue = cronDefaults) Map<String, Object> cronParams) {

        Task cronTask = new Task(name, cypherQueryString, cypherQueryParams);
        CronScheduler cronSchedule = new CronScheduler();
        cronSchedule.start(name, cron, 0, cronTask);

        Map<String, Object> cronObjectTmp = new HashMap<String, Object>();
        cronObjectTmp.put("name", name);
        cronObjectTmp.put("cronString", cron);
        cronObjectTmp.put("cronRunOk", 0);
        cronObjectTmp.put("cronRunError", 0);
        cronObjectTmp.put("cypherQuery", cypherQueryString);
        cronObjectTmp.put("cypherParams", cypherQueryParams);
        cronObjectTmp.put("cronTask", cronTask);
        cronObjectTmp.put("cronSchedule", cronSchedule);
        cronMap.addToMap(name, cronObjectTmp);

        return Stream.of(cronMap.getMapElementByNameClean(name)).map(MapResult::new);

    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure()
    @Description("call sc.cypher.listCron() - list all jobs")
    public Stream<MapResult> runCron(
            @Name("name") String name
    ) {
        Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(name);
        CronScheduler cronScheduler = (CronScheduler) cronObjectTmp.get("cronSchedule");
        return null;
    }

    // ----------------------------------------------------------------------------------
    // util
    // ----------------------------------------------------------------------------------
    /**
     * public static void main(String[] args) { CronScheduler cs = new
     * CronScheduler(); cs.toString(); Task aa = new Task("aa"); Task bb = new
     * Task("bb");
     *
     * cs.start("aaaa", "* * * * *", 10, aa); cs.start("bb", "* * * * * ", 1,
     * bb); // cs.stop("aaaa");
     *
     * }
     */
    public class CronScheduler {

        // --- Cron scheduler map
        //private Map<String, Scheduler> cronJobMap;
        Scheduler cronScheduler;

        // --- Cron scheduler initialization
        public CronScheduler() {
            cronScheduler = new Scheduler();
        }

        // --- Cron scheduler start
        public Scheduler start(String cronName, String cronSchedule, int taskRunDelay, Task cronTask) {
            // log.info(" CronScheduler start. " + cronName + " " + cronSchedule + " " + taskRunDelay);
            cronScheduler.schedule(cronSchedule, new Runnable() {
                // --- run task
                public void run() {
                    // --- run delay
                    try {
                        //     log.info(cronJobMap.toString());
                        //log.info("CronScheduler run " + cronName + " delay: " + taskRunDelay);
                        Thread.sleep(taskRunDelay * 1000);

                        cronTask.run();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            //cronJobMap.put(cronName, cronScheduler);
            cronScheduler.start();
            return cronScheduler;
        }

        // --- Cron scheduler stop
        public void stop(String cronName) {
            log.info(" CronScheduler stop ..." + cronName);
            cronScheduler.stop();
            cronScheduler.deschedule(cronScheduler.getTask(cronName));

        }

        // --- Cron scheduler stop
        public void run(String cronName) {
            log.info(" CronScheduler run ..." + cronName);
            //return cronScheduler.launch(cronScheduler.getTask(cronName));
        }
    }

    public class Task {

        public final String taskName;
        public final String cypherQueryString;
        public final Map<String, Object> cypherQueryParams;

        public Task(String tn, String cqs, Map<String, Object> cqp) {
            taskName = tn;
            cypherQueryString = cqs;
            cypherQueryParams = cqp;
            log.info("Task init " + taskName);
        }

        public void run() {
            log.info("Task run " + taskName);
            Map<String, Object> cronObjectTmp = cronMap.getMapElementByName(taskName);
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(cypherQueryString, cypherQueryParams);
                log.info("Task run  dbResult" + dbResult.resultAsString());

                cronObjectTmp.put("cronRunOk", 1 + (int) cronObjectTmp.get("cronRunOk"));
                tx.success();
            } catch (Exception ex) {
                cronObjectTmp.put("cronRunError", 1 + (int) cronObjectTmp.get("cronRunError"));
                cronObjectTmp.put("cronRunErrorMessage", "Task run error" + taskName + ex.toString());
                log.info("Task run error" + taskName + ex.toString());
            }
        }
    }

}
