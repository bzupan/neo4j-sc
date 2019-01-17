package sc.commit;

/**
 *
 * @author bz
 */
import java.util.HashMap;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import sc.MapProcess;

public class TrigerExecutor implements Runnable {
    
    private static TransactionData td;
    private static GraphDatabaseService db;
    private Log log;
    private String comitTrigger;
    private MapProcess nodePropertieTrigger;
    
    public TrigerExecutor(TransactionData transactionData, GraphDatabaseService graphDatabaseService, String beforeafter, MapProcess nodePropertieTriggerRequest, Log logger) {
        td = transactionData;
        db = graphDatabaseService;
        comitTrigger = beforeafter;
        nodePropertieTrigger = nodePropertieTriggerRequest;
        log = logger;
    }
    
    @Override
    public void run() {
        Map<String, Object> transactionMap = new HashMap<String, Object>();
        
        transactionMap.put("comitTrigger", comitTrigger);
        transactionMap.put("createdNodes", td.createdNodes());
        transactionMap.put("deletedNodes ", td.deletedNodes());
        transactionMap.put("assignedLabels ", td.assignedLabels());
        transactionMap.put("assignedNodeProperties ", td.assignedNodeProperties());
        transactionMap.put("removedNodeProperties ", td.removedNodeProperties());
        transactionMap.put("assignedRelationshipProperties ", td.removedLabels());
        
        transactionMap.put("assignedRelationshipProperties ", td.createdRelationships());
        transactionMap.put("deletedRelationships ", td.deletedRelationships());
        transactionMap.put("assignedRelationshipProperties ", td.assignedRelationshipProperties());
        transactionMap.put("removedRelationshipProperties ", td.removedRelationshipProperties());
        log.info("sc.TrigerExecutor transactionData: " + transactionMap);
//        System.out.println(status + " createdNodes " + td.createdNodes().toString());
//        System.out.println(status + " deletedNodes " + td.deletedNodes().toString());
//        System.out.println(status + " assignedLabels " + td.assignedLabels().toString());
//        System.out.println(status + " assignedNodeProperties " + td.assignedNodeProperties().toString());
//
//        System.out.println(status + " assignedRelationshipProperties " + td.removedLabels().toString());
//        System.out.println(status + " removedNodeProperties " + td.removedNodeProperties().toString());
//
//        System.out.println(status + " assignedRelationshipProperties " + td.createdRelationships().toString());
//        System.out.println(status + " deletedRelationships " + td.deletedRelationships().toString());
//        System.out.println(status + " assignedRelationshipProperties " + td.assignedRelationshipProperties().toString());
//        System.out.println(status + " removedRelationshipProperties " + td.removedRelationshipProperties().toString());

        Iterator<PropertyEntry<Node>> assignedNodePropertiesIterator = td.assignedNodeProperties().iterator();
        while (assignedNodePropertiesIterator.hasNext()) {

            // --- get propertieModified
            PropertyEntry<Node> updateNodePropertie = assignedNodePropertiesIterator.next();
            Map<String, Object> updateNodePropertieTriggerAction = nodePropertieTrigger.getMapElementByName((String) updateNodePropertie.key());
            
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
        
    }
}
