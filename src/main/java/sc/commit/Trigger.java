package sc.commit;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import org.neo4j.logging.internal.LogService;
import sc.MapProcess;

import sc.RunCypherQuery;
import sc.commit.TrigerExecutor;

/**
 * @author mh
 * @since 20.09.16
 *
 *
 *
 *
 */
//MATCH (n:test) DETACH DELETE n
//
//CREATE (n:test {propertieKey:123})
//MATCH  (n:test) RETURN n
//
//RETURN sc.commit.registerNodePropertieTriggerAfter('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredAfter=true')
//CREATE (n:test {propertieKey:123}) RETURN n
//
//RETURN sc.commit.registerNodePropertieTriggerBefore('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredBefore=true')
//CREATE (n:test {propertieKey:123}) RETURN n
//MATCH (n:test) SET n.propertieKey = "new" RETURN n
//
//MATCH (n:test) RETURN n
public class Trigger {

    private static final MapProcess nodePropertieTriggerAfter = new MapProcess();
    private static final MapProcess nodePropertieTriggerBefore = new MapProcess();

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.commit.registerNodePropertieTriggerAfter('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredAfter=true') // cypher properties {node=Node[1116], valueOld=oldValue, id=1116, value=1232, key=propertieKey, labels=[test]}")
    public Map<String, Object> registerNodePropertieTriggerAfter(
            @Name("propertieKey") String propertieKey,
            @Name("cypherQuery") String cypherQuery
    ) {
        // add crontask to cron map
        Map<String, Object> cypherObjectTmp = new HashMap<String, Object>();
        cypherObjectTmp.put("name", propertieKey);
        cypherObjectTmp.put("type", "nodePropertieTriggerAfter");
        cypherObjectTmp.put("propertieKey", propertieKey);
        cypherObjectTmp.put("cypherQuery", cypherQuery);
        nodePropertieTriggerAfter.addToMap(propertieKey, cypherObjectTmp);

        log.debug("sc.commit.registerNodePropertieTriggerAfter: " + cypherObjectTmp.toString());
        log.info("sc.commit.registerNodePropertieTriggerAfter: " + propertieKey);
        return nodePropertieTriggerAfter.getMapElementByNameClean(propertieKey);
    }

    @UserFunction
    @Description("RETURN sc.commit.registerNodePropertieTriggerAfter('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredAfter=true') // cypher properties {node=Node[1116], valueOld=oldValue, id=1116, value=1232, key=propertieKey, labels=[test]}")
    public Map<String, Object> removeNodePropertieTriggerAfter(
            @Name("propertieKey") String propertieKey
    ) {
        nodePropertieTriggerAfter.removeFromMap(propertieKey);
        log.info("sc.commit.removeNodePropertieTriggerAfter: " + propertieKey);
        return nodePropertieTriggerAfter.getMapElementByNameClean(propertieKey);
    }

    @UserFunction
    @Description("RETURN sc.commit.registerNodePropertieTriggerBefore('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredBefore=true') // cypher properties {node=Node[1116], valueOld=oldValue, id=1116, value=1232, key=propertieKey, labels=[test]}")
    public Map<String, Object> registerNodePropertieTriggerBefore(
            @Name("propertieKey") String propertieKey,
            @Name("cypherQuery") String cypherQuery
    ) {
        // add crontask to cron map
        Map<String, Object> cypherObjectTmp = new HashMap<String, Object>();
        cypherObjectTmp.put("name", propertieKey);
        cypherObjectTmp.put("type", "nodePropertieTriggerAfter");
        cypherObjectTmp.put("propertieKey", propertieKey);
        cypherObjectTmp.put("cypherQuery", cypherQuery);
        nodePropertieTriggerBefore.addToMap(propertieKey, cypherObjectTmp);

        log.debug("sc.commit.nodePropertieTriggerBefore: " + cypherObjectTmp.toString());
        log.info("sc.commit.nodePropertieTriggerBefore: " + propertieKey);
        return nodePropertieTriggerBefore.getMapElementByNameClean(propertieKey);
    }

    @UserFunction
    @Description("RETURN sc.commit.removeNodePropertieTriggerBefore('propertieKey', 'MATCH (n) WHERE ID(n)=$id SET n.triggeredAfter=true') // cypher properties {node=Node[1116], valueOld=oldValue, id=1116, value=1232, key=propertieKey, labels=[test]}")
    public Map<String, Object> removeNodePropertieTriggerBefore(
            @Name("propertieKey") String propertieKey
    ) {
        nodePropertieTriggerBefore.removeFromMap(propertieKey);
        log.info("sc.commit.removeNodePropertieTriggerBefore: " + propertieKey);
        return nodePropertieTriggerBefore.getMapElementByNameClean(propertieKey);
    }

    @UserFunction
    @Description("RETURN sc.commit.listTriggers() // list all CYPHER java VM calls")
    public Map<String, List< Map<String, Object>>> listTriggers() {
        log.debug("sc.cypher.listVm: " + nodePropertieTriggerAfter.getListFromMapAllClean().toString());
        Map<String, List< Map<String, Object>>> triggers = new HashMap();
        triggers.put("nodePropertieTriggerAfter", nodePropertieTriggerAfter.getListFromMapAllClean());
        triggers.put("nodePropertieTriggerBefore", nodePropertieTriggerBefore.getListFromMapAllClean());
        return triggers; //.map(CronJob::new);
    }

    public static class TriggerHandler implements TransactionEventHandler {

        @Context
        public GraphDatabaseService db;
        private static ExecutorService ex;
        public static LogService logsvc;
        private Log log;

        public TriggerHandler(GraphDatabaseService graphDatabaseService, ExecutorService executor, LogService logsvc) {
            db = graphDatabaseService;
            ex = executor;
            log = logsvc.getUserLog(Trigger.class);

        }

        @Override
        public Object beforeCommit(TransactionData transactionData) throws Exception {
            log.info("sc TriggerHandler " + "beforeCommit");
            String comitTrigger = "before";
            // ex.submit(new TrigerExecutor(transactionData, db, "beforeCommit", nodePropertieTriggerBefore, log));
            Iterator<PropertyEntry<Node>> assignedNodePropertiesIterator = transactionData.assignedNodeProperties().iterator();
            while (assignedNodePropertiesIterator.hasNext()) {

                // --- get propertieModified
                PropertyEntry<Node> updateNodePropertie = assignedNodePropertiesIterator.next();
                Map<String, Object> updateNodePropertieTriggerAction = nodePropertieTriggerBefore.getMapElementByName((String) updateNodePropertie.key());

                if (!(updateNodePropertieTriggerAction == null)) {
                    Transaction tx = db.beginTx();
                    try {
                        log.info(" update detected " + updateNodePropertieTriggerAction + " " + updateNodePropertie.key() + " " + updateNodePropertie.entity().getId());

                        Map<String, Object> cypherParams = new HashMap<String, Object>();
                        cypherParams.put("node", updateNodePropertie.entity());
                        cypherParams.put("id", updateNodePropertie.entity().getId());
                        cypherParams.put("labels", updateNodePropertie.entity().getLabels());
                        cypherParams.put("key", updateNodePropertie.key());
                        cypherParams.put("value", updateNodePropertie.value());
                        cypherParams.put("valueOld", updateNodePropertie.previouslyCommitedValue());
                        System.out.println(cypherParams);

                        String cypherQueryString = (String) updateNodePropertieTriggerAction.get("cypherQuery");

                        log.info("--- " + comitTrigger + " cypherQueryString ---------------\n" + cypherQueryString + "\n--- cypherParams ---------------\n" + cypherParams);
                        Result dbResult = db.execute(cypherQueryString, cypherParams);
                        log.info("--- " + comitTrigger + " cypherQueryString ---------------\n" + dbResult.resultAsString());
                        tx.success();
                    } catch (Exception e) {
                        tx.failure();
                    } finally {
                        tx.close();
                    }

                } else {
                    log.info("--- " + comitTrigger + " not registred key: " + updateNodePropertie.key() + " node id " + updateNodePropertie.entity().getId());
                }

            }
            return null;
        }

        @Override
        public void afterCommit(TransactionData transactionData, Object o) {
            log.info("sc TriggerHandler " + "afterCommit");
            ex.submit(new TrigerExecutor(transactionData, db, "afterCommit", nodePropertieTriggerAfter, log));
        }

        @Override
        public void afterRollback(TransactionData transactionData, Object o) {
            log.info("sc TriggerHandler " + "afterRollback");
        }
    }

}
