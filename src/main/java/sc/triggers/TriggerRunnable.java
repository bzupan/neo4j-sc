/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sc.triggers;

/**
 *
 * @author bz
 */
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.Log;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class TriggerRunnable implements Runnable {

    private static TransactionData td;
    private static GraphDatabaseService db;
    private Log log;

    public TriggerRunnable(TransactionData transactionData, GraphDatabaseService graphDatabaseService) {
        td = transactionData;
        db = graphDatabaseService;
        System.out.println("ss TriggerRunnable ssssssssssssssssss");

    }

    @Override
    public void run() {

        System.out.println("ss runnnnn ssssssssssssssssss");
        String status = "after";
        System.out.println(status + " createdNodes " + td.createdNodes().toString());
        System.out.println(status + " deletedNodes " + td.deletedNodes().toString());
        System.out.println(status + " assignedLabels " + td.assignedLabels().toString());
        System.out.println(status + " assignedNodeProperties " + td.assignedNodeProperties().toString());

        System.out.println(status + " assignedRelationshipProperties " + td.removedLabels().toString());
        System.out.println(status + " removedNodeProperties " + td.removedNodeProperties().toString());

        System.out.println(status + " assignedRelationshipProperties " + td.createdRelationships().toString());
        System.out.println(status + " deletedRelationships " + td.deletedRelationships().toString());
        System.out.println(status + " assignedRelationshipProperties " + td.assignedRelationshipProperties().toString());
        System.out.println(status + " removedRelationshipProperties " + td.removedRelationshipProperties().toString());

        Iterator <Node> crunchifyIterator = td.createdNodes().iterator();
        while (crunchifyIterator.hasNext()) {
           // log.info("A new node has been created!");
            System.out.println(crunchifyIterator.next().getId());
            crunchifyIterator.remove();
        }

//        try (Transaction tx = db.beginTx()) {
//            
//                     log.info("ex run  ex ex !");
//         System.out.println("ssssssssssssssssssss");
//            
//            Set<Node> suspects = new HashSet<>();
//            for (Node node : td.createdNodes()) {
//                if (node.hasLabel(Label.label("aa"))) {
//                    suspects.add(node);
//                    //GmailSender.sendEmail("maxdemarzi@gmail.com", "A new Suspect has been created in the System!", "boo-yeah");
//                    log.info("A new Suspect has been created!");
//                }
//            }
//
//            for (LabelEntry labelEntry : td.assignedLabels()) {
//                if (labelEntry.label().name().equals(Label.label("aa")) && !suspects.contains(labelEntry.node())) {
//                    log.info("A new Suspect has been identified!");
//                    suspects.add(labelEntry.node());
//                }
//            }
//
//            for (Relationship relationship : td.createdRelationships()) {
//                if (relationship.isType( RelationshipType.withName("KNOWS"))) {
//                    for (Node user : relationship.getNodes()) {
//                        if (user.hasLabel(Label.label("aa"))) {
//                            System.out.println("A new direct relationship to a Suspect has been created!");
//                        }
//
//                        for (Relationship knows : user.getRelationships(Direction.BOTH,RelationshipType.withName("KNOWS"))) {
//                            Node otherUser = knows.getOtherNode(user);
//                            if (otherUser.hasLabel(Label.label("aa")) && !otherUser.equals(relationship.getOtherNode(user))) {
//                                System.out.println("A new indirect relationship to a Suspect has been created!");
//                            }
//                        }
//                    }
//                }
//            }
//        }
    }
}
