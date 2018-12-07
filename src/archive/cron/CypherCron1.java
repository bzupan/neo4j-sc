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
import sc.MapResult;

public class CypherCron1 {

    // private static final List< CronJob> cronList = new ArrayList<CronJob>();
    private static final ArrayList<Map<String, Object>> cronList = new ArrayList<Map<String, Object>>();

    private static final CronScheduler cs = new CronScheduler();

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    //  @UserFunction
    @Procedure()
    @Description("call sc.cypher.listCronJobs() - list all cron jobs")
    public Stream<Map<String, Object>> listCronJobs() {
        return cronList.stream(); //.map(CronJob::new);
    }

    @Procedure()
    @Description("call sc.cypher.deleteCronJob('cronName') - remove cron job")
    public Stream<Map<String, Object>> deleteCronJob(@Name("name") String name) {
        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cronList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " remove " + name);
            cs.stop(name);
            cronList.remove(cypherQueryMap);
        }

        return Stream.empty();
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cypher.addCronJob('cronTaskName','* * * * *','MATCH (n) RETURN n', {})   submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
    public Stream<Map<String, Object>> addCronJob(
            @Name("name") String name,
            @Name("cron") String cron,
            @Name("cypherQueryString") String cypherQueryString,
            @Name(value = "cypherQueryParams", defaultValue = "{}") Map<String, Object> cypherQueryParams) {
//        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
//        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement, params)), 0, rate);

        // cronMap = new HashMap<>();
        Map<String, Object> cronObject = new HashMap<String, Object>(); //new CronJob(cron, name, cypherQueryString, cypherQueryParams);
        cronObject.put("cronName", name);
        cronObject.put("cronString", cron);
        cronObject.put("cypherQuery", cypherQueryString);
        cronObject.put("cypherParams", cypherQueryParams);

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cronList, "name", name);
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " remove " + name);
            cs.stop(name);
            cronList.remove(cypherQueryMap);
        }

        cronList.add(cronObject);
        Task cronTask = new Task(name, cypherQueryString, cypherQueryParams);
        cs.start(name, cron, 0, cronTask);
        return  Stream.empty(); //Stream.of(cronObject);
    }

    /**
     * public class CronJob { // static
     *
     * public String cronString; public final String cronName; public String
     * cypherQuery; public Object cypherParams;
     *
     * // four constructors public CronJob(String cs, String cn, String cq,
     * Object ce) { this.cronString = cs; this.cronName = cn; this.cypherQuery =
     * cq; this.cypherParams = ce; }
     *
     * public CronJob(String cs) { this.cronName = cs; // this.cronScheduler =
     * new Scheduler(); }
     *
     * public CronJob addCron() { return this; }
     *
     * public CronJob getCron() { return this; }
     *
     * public CronJob update(String cs, String cn, String cq, Object ce) {
     * this.cronString = cs; //this.cronName = cn; this.cypherQuery = cq;
     * this.cypherParams = ce;
     *
     * //this.cronScheduler.stop(); return this; }
     *
     * @Override public boolean equals(Object o) { return this == o || o
     * instanceof CronJob && cronName.equals(((CronJob) o).cronName); }
     *
     * @Override public int hashCode() { return cronName.hashCode(); }
     *
     * }
     *
     *
     * public static void main(String[] args) { CronScheduler cs = new
     * CronScheduler(); cs.toString(); Task aa = new Task("aa"); Task bb = new
     * Task("bb");
     *
     * cs.start("aaaa", "* * * * *", 10, aa); cs.start("bb", "* * * * * ", 1,
     * bb); // cs.stop("aaaa");
     *
     * }
     */
    public static class CronScheduler {

        // --- Cron scheduler map
        private Map<String, Scheduler> cronJobMap;

        // --- Cron scheduler initialization
        public CronScheduler() {
            cronJobMap = new HashMap<String, Scheduler>();
            // log.info(" CronScheduler inittialization");
        }

        // --- Cron scheduler start
        public CronScheduler start(String cronName, String cronSchedule, int taskRunDelay, Task cronTask) {
            // log.info(" CronScheduler start. " + cronName + " " + cronSchedule + " " + taskRunDelay);
            Scheduler cronScheduler = new Scheduler();
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
            cronJobMap.put(cronName, cronScheduler);
            cronScheduler.start();
            return null;
        }

        // --- Cron scheduler stop
        public CronScheduler stop(String cronName) {
            // log.info(" CronScheduler stop ..." + cronName);
            Scheduler cronScheduler = cronJobMap.get(cronName);
            cronScheduler.stop();
            cronJobMap.remove(cronName);
            return null;
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
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(cypherQueryString, cypherQueryParams);
                log.info("Task run  dbResult" + dbResult.resultAsString());
                tx.success();
            } catch (Exception ex) {
                log.info("Task run error" + taskName + ex.toString());
            }
        }
    }

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

}
