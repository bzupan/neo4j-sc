/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sc.triggers;

import java.util.concurrent.ExecutorService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.Log;

/**
 *
 * @author bz
 */
public class ScTransactionEventHandler implements TransactionEventHandler {

    public static GraphDatabaseService db;
    private static ExecutorService ex;

    public ScTransactionEventHandler(GraphDatabaseService graphDatabaseService, ExecutorService executor, LogService log) {
        db = graphDatabaseService;
        ex = executor;
    }

    @Override
    public Object beforeCommit(TransactionData transactionData) throws Exception {
        String status = "beforeCommit";
        System.out.println(status + " createdNodes " + transactionData.createdNodes().toString());
        System.out.println(status + " deletedNodes " + transactionData.deletedNodes().toString());
        System.out.println(status + " assignedLabels " + transactionData.assignedLabels().toString());
        System.out.println(status + " assignedNodeProperties " + transactionData.assignedNodeProperties().toString());

        System.out.println(status + " assignedRelationshipProperties " + transactionData.removedLabels().toString());
        System.out.println(status + " removedNodeProperties " + transactionData.removedNodeProperties().toString());

        System.out.println(status + " assignedRelationshipProperties " + transactionData.createdRelationships().toString());
        System.out.println(status + " deletedRelationships " + transactionData.deletedRelationships().toString());
        System.out.println(status + " assignedRelationshipProperties " + transactionData.assignedRelationshipProperties().toString());
        System.out.println(status + " removedRelationshipProperties " + transactionData.removedRelationshipProperties().toString());

        // System.out.println(status + " assignedLabels "  + transactionData.toString());
        return null;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Object o) {
        System.out.println("afterCommit");
        ex.submit(new TriggerRunnable(transactionData, db));
        
        System.out.println("ex ex ex !");
    }

    @Override
    public void afterRollback(TransactionData transactionData, Object o) {
        System.out.println("afterRollback");
    }
}
