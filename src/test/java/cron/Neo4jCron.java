package cron;



// http://www.sauronsoftware.it/projects/cron4j/manual.php
// https://mvnrepository.com/artifact/it.sauronsoftware.cron4j/cron4j/2.2.5
// https://github.com/Takuto88/cron4j
//import apoc.Pools;
//import apoc.util.Util;
import sc.*;
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

public class Neo4jCron {

    private static final List< CronJob> cronList = new ArrayList<CronJob>();

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    //  @UserFunction
    @Procedure()
    @Description("call sc.cronList() - list all jobs")
    public Stream<CronJob> cronList() {
        return cronList.stream(); //.map(CronJob::new);
    }

    @Procedure()
    @Description("call sc.cronRemove('cronName') - list all jobs")
    public Stream<CronJob> cronRemove(@Name("name") String name) {
        CronJob info = new CronJob(name);
        Object future = cronList.remove(info);

        if (future != null) {
            //   future.cancel(true);
            //  return Stream.of(info.update(future));
        }
        return Stream.empty();
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.cron('cronName','MATCH (n) RETURN n','* * * * *', {})   submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
    public Stream<CronJob> cron(
            @Name("name") String name,
            @Name("statement") String statement,
            @Name("cron") String cron,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
//        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement, params)), 0, rate);

        // cronMap = new HashMap<>();
        CronJob cronObject = new CronJob(cron, name, statement, true);
        cronList.add(cronObject);
        return Stream.of(cronObject);
    }

    public class CronJob { // static

        public String cronString;
        public final String cronName;
        public String cypherQuery;
        public boolean cronEnabled;

        // Creates a Scheduler instance.
        

        // four constructors
        public CronJob(String cs, String cn, String cq, boolean ce) {
            this.cronString = cs;
            this.cronName = cn;
            this.cypherQuery = cq;
            this.cronEnabled = ce;
            String selfName = this.cronName;
            // Schedule a once-a-minute task.
             Scheduler cronScheduler = new Scheduler();
            cronScheduler.schedule(this.cronString, new Runnable() {
                //String selfName = this.cronName;
                public void run() {
                    log.info("Another minute ticked away..." + selfName);
                    //  System.out.println("Another minute ticked away...");
                }
            });
            // Starts the scheduler.
           cronScheduler.start();
            // Will run for ten minutes.

        }

        public CronJob(String cs) {
            this.cronName = cs;
        }

        public CronJob addCron() {
            return this;
        }

        public CronJob getCron() {
            return this;
        }

        public CronJob update(String cs, String cn, String cq, boolean ce) {
            this.cronString = cs;
            //this.cronName = cn;
            this.cypherQuery = cq;
            this.cronEnabled = ce;

            //this.cronScheduler.stop();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof CronJob && cronName.equals(((CronJob) o).cronName);
        }

        @Override
        public int hashCode() {
            return cronName.hashCode();
        }

    }

}

//        public static class JobInfo {
//
//            public final String name;
//            public long delay;
//            public long rate;
//            public boolean done;
//            public boolean cancelled;
//
//            public JobInfo(String name) {
//                this.name = name;
//            }
//
//            public JobInfo(String name, long delay, long rate) {
//                this.name = name;
//                this.delay = delay;
//                this.rate = rate;
//            }
//
//            public JobInfo update(Future future) {
//                this.done = future.isDone();
//                this.cancelled = future.isCancelled();
//                return this;
//            }
//
//            @Override
//            public boolean equals(Object o) {
//                return this == o || o instanceof JobInfo && name.equals(((JobInfo) o).name);
//            }
//
//            @Override
//            public int hashCode() {
//                return name.hashCode();
//            }
//        }
//    package example ;
//
//    import it.sauronsoftware.cron4j.SchedulingPattern ;
//    import it.sauronsoftware.cron4j.Task ;
//    import it.sauronsoftware.cron4j.TaskCollector ;
//    import it.sauronsoftware.cron4j.TaskTable ;
//
//    /**
//     * The custom TaskCollector used to retrieve the task list. This sample
//     * implementation returns always the same task that the scheduler executes
//     * once a minute.
//     */
//    public class MyTaskCollector implements TaskCollector {
//
//        public TaskTable getTasks() {
//            SchedulingPattern pattern = new SchedulingPattern("* * * * *");
//            Task task = new MyTask();
//            TaskTable ret = new TaskTable();
//            ret.add(pattern, task);
//            return ret;
//        }
//
//    }
//
//    package example ;
//
//    import it.sauronsoftware.cron4j.Scheduler ;
//    import it.sauronsoftware.cron4j.TaskCollector ;
//
//    import javax.servlet.ServletContext ;
//    import javax.servlet.ServletContextEvent ;
//    import javax.servlet.ServletContextListener ;
//
//    /**
//     * This listener starts a scheduler bounded to the web application: the
//     * scheduler is started when the application is started, and the scheduler
//     * is stopped when the application is destroyed. The scheduler uses a custom
//     * TaskCollector to retrieve, once a minute, its job list. Moreover the
//     * scheduler is registered on the application context, in a attribute named
//     * according to the value of the {@link Constants#SCHEDULER} constant.
//     */
//    public class SchedulerServletContextListener implements ServletContextListener {
//
//        public void contextInitialized(ServletContextEvent event) {
//            ServletContext context = event.getServletContext();
//            // 1. Creates the scheduler.
//            Scheduler scheduler = new Scheduler();
//            // 2. Registers a custom task collector.
//            TaskCollector collector = new MyTaskCollector();
//            scheduler.addTaskCollector(collector);
//            // 3. Starts the scheduler.
//            scheduler.start();
//            // 4. Registers the scheduler.
//            context.setAttribute(Constants.SCHEDULER, scheduler);
//        }
//
//        public void contextDestroyed(ServletContextEvent event) {
//            ServletContext context = event.getServletContext();
//            // 1. Retrieves the scheduler from the context.
//            Scheduler scheduler = (Scheduler) context.getAttribute(Constants.SCHEDULER);
//            // 2. Removes the scheduler from the context.
//            context.removeAttribute(Constants.SCHEDULER);
//            // 3. Stops the scheduler.
//            scheduler.stop();
//        }
//
//    }
//
//    public class Quickstart {
//
//        public static void main(String[] args) {
//            // Creates a Scheduler instance.
//            Scheduler s = new Scheduler();
//            // Schedule a once-a-minute task.
//            s.schedule("* * * * *", new Runnable() {
//                public void run() {
//                    System.out.println("Another minute ticked away...");
//                }
//            });
//            // Starts the scheduler.
//            s.start();
//            // Will run for ten minutes.
//            try {
//                Thread.sleep(1000L * 60L * 10L);
//            } catch (InterruptedException e) {
//                ;
//            }
//            // Stops the scheduler.
//            s.stop();
//        }
//
//    }
//
//    @Description("apoc.periodic.repeat('name',statement,repeat-rate-in-seconds, config) submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
//    public Stream<JobInfo> repeat(
//            @Name("name") String name,
//            @Name("statement") String statement,
//            @Name("rate") long rate,
//            @Name(value = "config",
//                    defaultValue = "{}"
//            ) Map<String, Object> config) {
//        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
//        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement, params)), 0, rate);
//        return Stream.of(info);
//    }
//
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.commit(statement,params) - runs the given statement in separate transactions until it returns 0")
//    public Stream<RundownResult> commit(@Name("statement") String statement, @Name("params") Map<String, Object> parameters) throws ExecutionException, InterruptedException {
//        Map<String, Object> params = parameters == null ? Collections.emptyMap() : parameters;
//        long total = 0, executions = 0, updates = 0;
//        long start = nanoTime();
//
//        AtomicInteger batches = new AtomicInteger();
//        AtomicInteger failedCommits = new AtomicInteger();
//        Map<String, Long> commitErrors = new ConcurrentHashMap<>();
//        AtomicInteger failedBatches = new AtomicInteger();
//        Map<String, Long> batchErrors = new ConcurrentHashMap<>();
//
//        do {
//            Map<String, Object> window = Util.map("_count", updates, "_total", total);
//            updates = Util.getFuture(Pools.SCHEDULED.submit(() -> {
//                batches.incrementAndGet();
//                try {
//                    return executeNumericResultStatement(statement, merge(window, params));
//                } catch (Exception e) {
//                    failedBatches.incrementAndGet();
//                    recordError(batchErrors, e);
//                    return 0L;
//                }
//            }), commitErrors, failedCommits, 0L);
//            total += updates;
//            if (updates > 0) {
//                executions++;
//            }
//        } while (updates > 0 && !Util.transactionIsTerminated(terminationGuard));
//        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start);
//        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
//        return Stream.of(new RundownResult(total, executions, timeTaken, batches.get(), failedBatches.get(), batchErrors, failedCommits.get(), commitErrors, wasTerminated));
//    }
//
//    private void recordError(Map<String, Long> executionErrors, Exception e) {
//        executionErrors.compute(getMessages(e), (s, i) -> i == null ? 1 : i + 1);
//    }
//
//    private String getMessages(Throwable e) {
//        Set<String> errors = new LinkedHashSet<>();
//        do {
//            errors.add(e.getMessage());
//            e = e.getCause();
//        } while (e.getCause() != null && !e.getCause().equals(e));
//        return String.join("\n", errors);
//    }
//
//    public static class RundownResult {
//
//        public final long updates;
//        public final long executions;
//        public final long runtime;
//        public final long batches;
//        public final long failedBatches;
//        public final Map<String, Long> batchErrors;
//        public final long failedCommits;
//        public final Map<String, Long> commitErrors;
//        public final boolean wasTerminated;
//
//        public RundownResult(long total, long executions, long timeTaken, long batches, long failedBatches, Map<String, Long> batchErrors, long failedCommits, Map<String, Long> commitErrors, boolean wasTerminated) {
//            this.updates = total;
//            this.executions = executions;
//            this.runtime = timeTaken;
//            this.batches = batches;
//            this.failedBatches = failedBatches;
//            this.batchErrors = batchErrors;
//            this.failedCommits = failedCommits;
//            this.commitErrors = commitErrors;
//            this.wasTerminated = wasTerminated;
//        }
//    }
//
//    private long executeNumericResultStatement(@Name("statement") String statement, @Name("params") Map<String, Object> parameters) {
//        long sum = 0;
//        try (Result result = db.execute(statement, parameters)) {
//            while (result.hasNext()) {
//                Collection<Object> row = result.next().values();
//                for (Object value : row) {
//                    if (value instanceof Number) {
//                        sum += ((Number) value).longValue();
//                    }
//                }
//            }
//        }
//        return sum;
//    }
//
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.submit('name',statement) - submit a one-off background statement")
//    public Stream<JobInfo> submit(@Name("name") String name, @Name("statement") String statement) {
//        JobInfo info = submit(name, () -> {
//            try {
//                Iterators.count(db.execute(statement));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        });
//        return Stream.of(info);
//    }
//
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.repeat('name',statement,repeat-rate-in-seconds, config) submit a repeatedly-called background statement. Fourth parameter 'config' is optional and can contain 'params' entry for nested statement.")
//    public Stream<JobInfo> repeat(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
//        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
//        JobInfo info = schedule(name, () -> Iterators.count(db.execute(statement, params)), 0, rate);
//        return Stream.of(info);
//    }
//
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.countdown('name',statement,repeat-rate-in-seconds) submit a repeatedly-called background statement until it returns 0")
//    public Stream<JobInfo> countdown(@Name("name") String name, @Name("statement") String statement, @Name("rate") long rate) {
//        JobInfo info = submit(name, new Countdown(name, statement, rate));
//        info.rate = rate;
//        return Stream.of(info);
//    }
//
//    /**
//     * Call from a procedure that gets a
//     * <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to
//     * the runnable.
//     */
//    public static <T> JobInfo submit(String name, Runnable task) {
//        JobInfo info = new JobInfo(name);
//        Future<T> future = list.remove(info);
//        if (future != null && !future.isDone()) {
//            future.cancel(false);
//        }
//
//        Future newFuture = Pools.SCHEDULED.submit(task);
//        list.put(info, newFuture);
//        return info;
//    }
//
//    /**
//     * Call from a procedure that gets a
//     * <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to
//     * the runnable.
//     */
//    public static JobInfo schedule(String name, Runnable task, long delay, long repeat) {
//        JobInfo info = new JobInfo(name, delay, repeat);
//        Future future = list.remove(info);
//        if (future != null && !future.isDone()) {
//            future.cancel(false);
//        }
//
//        ScheduledFuture<?> newFuture = Pools.SCHEDULED.scheduleWithFixedDelay(task, delay, repeat, TimeUnit.SECONDS);
//        list.put(info, newFuture);
//        return info;
//    }
//
//    /**
//     * as long as cypherLoop does not return 0, null, false, or the empty string
//     * as 'value' do:
//     *
//     * invoke cypherAction in batched transactions being feeded from
//     * cypherIteration running in main thread
//     *
//     * @param cypherLoop
//     * @param cypherIterate
//     * @param cypherAction
//     * @param batchSize
//     */
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.rock_n_roll_while('some cypher for knowing when to stop', 'some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
//    public Stream<LoopingBatchAndTotalResult> rock_n_roll_while(
//            @Name("cypherLoop") String cypherLoop,
//            @Name("cypherIterate") String cypherIterate,
//            @Name("cypherAction") String cypherAction,
//            @Name("batchSize") long batchSize) {
//
//        Stream<LoopingBatchAndTotalResult> allResults = Stream.empty();
//
//        Map<String, Object> loopParams = new HashMap<>(1);
//        Object value = null;
//
//        while (true) {
//            loopParams.put("previous", value);
//
//            try (Result result = db.execute(cypherLoop, loopParams)) {
//                value = result.next().get("loop");
//                if (!Util.toBoolean(value)) {
//                    return allResults;
//                }
//            }
//
//            log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
//            try (Result result = db.execute(cypherIterate)) {
//                Stream<BatchAndTotalResult> oneResult
//                        = iterateAndExecuteBatchedInSeparateThread((int) batchSize, false, false, 0, result, params -> db.execute(cypherAction, params), 50);
//                final Object loopParam = value;
//                allResults = Stream.concat(allResults, oneResult.map(r -> r.inLoop(loopParam)));
//            }
//        }
//    }
//
//    /**
//     * invoke cypherAction in batched transactions being feeded from
//     * cypherIteration running in main thread
//     *
//     * @param cypherIterate
//     * @param cypherAction
//     */
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.iterate('statement returning items', 'statement per item', {batchSize:1000,iterateList:true,parallel:false,params:{},concurrency:50,retries:0}) YIELD batches, total - run the second statement for each item returned by the first statement. Returns number of batches and total processed rows")
//    public Stream<BatchAndTotalResult> iterate(
//            @Name("cypherIterate") String cypherIterate,
//            @Name("cypherAction") String cypherAction,
//            @Name("config") Map<String, Object> config) {
//
//        long batchSize = Util.toLong(config.getOrDefault("batchSize", 10000));
//        int concurrency = Util.toInteger(config.getOrDefault("concurrency", 50));
//        boolean parallel = Util.toBoolean(config.getOrDefault("parallel", false));
//        boolean iterateList = Util.toBoolean(config.getOrDefault("iterateList", true));
//        long retries = Util.toLong(config.getOrDefault("retries", 0)); // todo sleep/delay or push to end of batch to try again or immediate ?
//        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
//        try (Result result = db.execute(cypherIterate, params)) {
//            Pair<String, Boolean> prepared = prepareInnerStatement(cypherAction, iterateList, result.columns(), "_batch");
//            String innerStatement = prepared.first();
//            iterateList = prepared.other();
//            log.info("starting batching from `%s` operation using iteration `%s` in separate thread", cypherIterate, cypherAction);
//            return iterateAndExecuteBatchedInSeparateThread((int) batchSize, parallel, iterateList, retries, result, (p) -> db.execute(innerStatement, merge(params, p)).close(), concurrency);
//        }
//    }
//
//    public long retry(Consumer<Map<String, Object>> executor, Map<String, Object> params, long retry, long maxRetries) {
//        try {
//            executor.accept(merge(params, singletonMap("_retry", retry)));
//            return retry;
//        } catch (Exception e) {
//            if (retry >= maxRetries) {
//                throw e;
//            }
//            log.warn("Retrying operation " + retry + " of " + maxRetries);
//            Util.sleep(100);
//            return retry(executor, params, retry + 1, maxRetries);
//        }
//    }
//
//    public Pair<String, Boolean> prepareInnerStatement(String cypherAction, boolean iterateList, List<String> columns, String iterator) {
//        String names = columns.stream().map(Util::quote).collect(Collectors.joining("|"));
//        boolean withCheck = regNoCaseMultiLine("[{$](" + names + ")\\}?\\s+AS\\s+").matcher(cypherAction).find();
//        if (withCheck) {
//            return Pair.of(cypherAction, false);
//        }
//        if (iterateList) {
//            if (regNoCaseMultiLine("UNWIND\\s+[{$]" + iterator + "\\}?\\s+AS\\s+").matcher(cypherAction).find()) {
//                return Pair.of(cypherAction, true);
//            }
//            String with = Util.withMapping(columns.stream(), (c) -> Util.quote(iterator) + "." + Util.quote(c) + " AS " + Util.quote(c));
//            return Pair.of("UNWIND " + Util.param(iterator) + " AS " + Util.quote(iterator) + with + " " + cypherAction, true);
//        }
//        return Pair.of(Util.withMapping(columns.stream(), (c) -> Util.param(c) + " AS " + Util.quote(c)) + cypherAction, false);
//    }
//
//    public Pattern regNoCaseMultiLine(String pattern) {
//        return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
//    }
//
//    @Procedure(mode = Mode.WRITE)
//    @Description("apoc.periodic.rock_n_roll('some cypher for iteration', 'some cypher as action on each iteration', 10000) YIELD batches, total - run the action statement in batches over the iterator statement's results in a separate thread. Returns number of batches and total processed rows")
//    public Stream<BatchAndTotalResult> rock_n_roll(
//            @Name("cypherIterate") String cypherIterate,
//            @Name("cypherAction") String cypherAction,
//            @Name("batchSize") long batchSize) {
//
//        log.info("starting batched operation using iteration `%s` in separate thread", cypherIterate);
//        try (Result result = db.execute(cypherIterate)) {
//            return iterateAndExecuteBatchedInSeparateThread((int) batchSize, false, false, 0, result, p -> db.execute(cypherAction, p).close(), 50);
//        }
//    }
//
//    private Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(int batchsize, boolean parallel, boolean iterateList, long retries,
//            Iterator<Map<String, Object>> iterator, Consumer<Map<String, Object>> consumer, int concurrency) {
//        ExecutorService pool = parallel ? Pools.DEFAULT : Pools.SINGLE;
//        List<Future<Long>> futures = new ArrayList<>(concurrency);
//        long batches = 0;
//        long start = System.nanoTime();
//        AtomicLong count = new AtomicLong();
//        AtomicInteger failedOps = new AtomicInteger();
//        AtomicLong retried = new AtomicLong();
//        Map<String, Long> operationErrors = new ConcurrentHashMap<>();
//        AtomicInteger failedBatches = new AtomicInteger();
//        Map<String, Long> batchErrors = new HashMap<>();
//        long successes = 0;
//        do {
//            if (Util.transactionIsTerminated(terminationGuard)) {
//                break;
//            }
//            if (log.isDebugEnabled()) {
//                log.debug("execute in batch no " + batches + " batch size " + batchsize);
//            }
//            List<Map<String, Object>> batch = Util.take(iterator, batchsize);
//            long currentBatchSize = batch.size();
//            Callable<Long> task;
//            if (iterateList) {
//                task = () -> {
//                    long c = count.addAndGet(currentBatchSize);
//                    if (Util.transactionIsTerminated(terminationGuard)) {
//                        return 0L;
//                    }
//                    try {
//                        Map<String, Object> params = Util.map("_count", c, "_batch", batch);
//                        retried.addAndGet(retry(consumer, params, 0, retries));
//                    } catch (Exception e) {
//                        failedOps.addAndGet(batchsize);
//                        recordError(operationErrors, e);
//                    }
//                    return currentBatchSize;
//                };
//            } else {
//                task = () -> {
//                    if (Util.transactionIsTerminated(terminationGuard)) {
//                        return 0L;
//                    }
//                    return batch.stream().map(
//                            p -> {
//                                long c = count.incrementAndGet();
//                                if (c % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) {
//                                    return 0;
//                                }
//                                try {
//                                    Map<String, Object> params = merge(p, Util.map("_count", c, "_batch", batch));
//                                    retried.addAndGet(retry(consumer, params, 0, retries));
//                                } catch (Exception e) {
//                                    failedOps.incrementAndGet();
//                                    recordError(operationErrors, e);
//                                }
//                                return 1;
//                            }).mapToLong(l -> l).sum();
//                };
//            }
//            futures.add(Util.inTxFuture(pool, db, task));
//            batches++;
//            if (futures.size() > concurrency) {
//                while (futures.stream().noneMatch(Future::isDone)) { // none done yet, block for a bit
//                    LockSupport.parkNanos(1000);
//                }
//                Iterator<Future<Long>> it = futures.iterator();
//                while (it.hasNext()) {
//                    Future<Long> future = it.next();
//                    if (future.isDone()) {
//                        successes += Util.getFuture(future, batchErrors, failedBatches, 0L);
//                        it.remove();
//                    }
//                }
//            }
//        } while (iterator.hasNext());
//        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
//        if (wasTerminated) {
//            successes += futures.stream().mapToLong(f -> Util.getFutureOrCancel(f, batchErrors, failedBatches, 0L)).sum();
//        } else {
//            successes += futures.stream().mapToLong(f -> Util.getFuture(f, batchErrors, failedBatches, 0L)).sum();
//        }
//        Util.logErrors("Error during iterate.commit:", batchErrors, log);
//        Util.logErrors("Error during iterate.execute:", operationErrors, log);
//        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
//        BatchAndTotalResult result
//                = new BatchAndTotalResult(batches, count.get(), timeTaken, successes, failedOps.get(), failedBatches.get(), retried.get(), operationErrors, batchErrors, wasTerminated);
//        return Stream.of(result);
//    }
//
//    public static class BatchAndTotalResult {
//
//        public final long batches;
//        public final long total;
//        public final long timeTaken;
//        public final long committedOperations;
//        public final long failedOperations;
//        public final long failedBatches;
//        public final long retries;
//        public final Map<String, Long> errorMessages;
//        public final Map<String, Object> batch;
//        public final Map<String, Object> operations;
//        public final boolean wasTerminated;
//
//        public BatchAndTotalResult(long batches, long total, long timeTaken, long committedOperations,
//                long failedOperations, long failedBatches, long retries,
//                Map<String, Long> operationErrors, Map<String, Long> batchErrors, boolean wasTerminated) {
//            this.batches = batches;
//            this.total = total;
//            this.timeTaken = timeTaken;
//            this.committedOperations = committedOperations;
//            this.failedOperations = failedOperations;
//            this.failedBatches = failedBatches;
//            this.retries = retries;
//            this.errorMessages = operationErrors;
//            this.wasTerminated = wasTerminated;
//            this.batch = Util.map("total", batches, "failed", failedBatches, "committed", batches - failedBatches, "errors", batchErrors);
//            this.operations = Util.map("total", total, "failed", failedOperations, "committed", committedOperations, "errors", operationErrors);
//        }
//
//        public LoopingBatchAndTotalResult inLoop(Object loop) {
//            return new LoopingBatchAndTotalResult(loop, batches, total);
//        }
//    }
//
//    public static class LoopingBatchAndTotalResult {
//
//        public Object loop;
//        public long batches;
//        public long total;
//
//        public LoopingBatchAndTotalResult(Object loop, long batches, long total) {
//            this.loop = loop;
//            this.batches = batches;
//            this.total = total;
//        }
//    }
//
//    /**
//     * Call from a procedure that gets a
//     * <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to
//     * the runnable.
//     */
//    public static JobInfo schedule(String name, Runnable task, long delay) {
//        JobInfo info = new JobInfo(name, delay, 0);
//        Future future = list.remove(info);
//        if (future != null) {
//            future.cancel(false);
//        }
//
//        ScheduledFuture<?> newFuture = Pools.SCHEDULED.schedule(task, delay, TimeUnit.SECONDS);
//        list.put(info, newFuture);
//        return info;
//    }
//
//    public static class JobInfo {
//
//        public final String name;
//        public long delay;
//        public long rate;
//        public boolean done;
//        public boolean cancelled;
//
//        public JobInfo(String name) {
//            this.name = name;
//        }
//
//        public JobInfo(String name, long delay, long rate) {
//            this.name = name;
//            this.delay = delay;
//            this.rate = rate;
//        }
//
//        public JobInfo update(Future future) {
//            this.done = future.isDone();
//            this.cancelled = future.isCancelled();
//            return this;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            return this == o || o instanceof JobInfo && name.equals(((JobInfo) o).name);
//        }
//
//        @Override
//        public int hashCode() {
//            return name.hashCode();
//        }
//    }
//
//    private class Countdown implements Runnable {
//
//        private final String name;
//        private final String statement;
//        private final long rate;
//
//        public Countdown(String name, String statement, long rate) {
//            this.name = name;
//            this.statement = statement;
//            this.rate = rate;
//        }
//
//        @Override
//        public void run() {
//            if (Periodic.this.executeNumericResultStatement(statement, Collections.emptyMap()) > 0) {
//                Pools.SCHEDULED.schedule(() -> submit(name, this), rate, TimeUnit.SECONDS);
//            }
//        }
//    }

