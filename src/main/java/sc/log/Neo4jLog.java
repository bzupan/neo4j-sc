package sc.log;

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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.Relationship;

public class Neo4jLog {

    private static final String logDefaults = "{loglevel:'info'}";

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.log.info('logString',logObject)  // ")
    public Map<String, Object> info(
            @Name("logString") Object logString,
            @Name(value = "logObject", defaultValue = "{}") Map<String, Object> logObject
    ) throws JsonProcessingException {
        String logObjectString = "";
        if (logObject instanceof Map) {
            ObjectMapper mapper = new ObjectMapper();
            logObjectString = mapper.writeValueAsString(logObject); //(String) logObject.toString();
        } else if (logObject instanceof Node) {
            ObjectMapper mapper = new ObjectMapper();
            Node neo4jNode = (Node) logObject;
            String neo4jNodeAsJson = "{_id:" + neo4jNode.getId() + ",_labels:" + neo4jNode.getLabels().toString() + ",_properties:" + mapper.writeValueAsString(neo4jNode.getAllProperties()) + "}";
            logObjectString = "node: " + neo4jNodeAsJson;
        } else if (logObject instanceof Relationship) {
            ObjectMapper mapper = new ObjectMapper();
            Relationship neo4jRelationship = (Relationship) logObject;
            String neo4jRelationshipAsJson = "{_id:" + neo4jRelationship.getId() + ",_from:" + neo4jRelationship.getStartNodeId() + ",_to:" + neo4jRelationship.getEndNodeId() + ",_type:" + neo4jRelationship.getType().toString() + ",_properties:" + mapper.writeValueAsString(neo4jRelationship.getAllProperties()) + "}";
            logObjectString = "relationship: " + neo4jRelationshipAsJson;
        } else {
            logObjectString = logObject.toString();
        }

        log.info("sc.log: " + logString.toString() + " " + logObjectString);
        return logObject;
    }

    @UserFunction
    @Description("RETURN sc.log.info('log info ')  // ")
    public Map<String, Object> infoObject(
            @Name("logString") Object logString,
            @Name(value = "logObject", defaultValue = "{}") Map<String, Object> logObject
    ) throws JsonProcessingException {
        String logObjectString = "";
        if (logObject instanceof Map) {
            ObjectMapper mapper = new ObjectMapper();
            logObjectString = mapper.writeValueAsString(logObject); //(String) logObject.toString();
        } else if (logObject instanceof Node) {
            ObjectMapper mapper = new ObjectMapper();
            Node neo4jNode = (Node) logObject;
            String neo4jNodeAsJson = "{_id:" + neo4jNode.getId() + ",_labels:" + neo4jNode.getLabels().toString() + ",_properties:" + mapper.writeValueAsString(neo4jNode.getAllProperties()) + "}";
            logObjectString = "node: " + neo4jNodeAsJson;
        } else if (logObject instanceof Relationship) {
            ObjectMapper mapper = new ObjectMapper();
            Relationship neo4jRelationship = (Relationship) logObject;
            String neo4jRelationshipAsJson = "{_id:" + neo4jRelationship.getId() + ",_from:" + neo4jRelationship.getStartNodeId() + ",_to:" + neo4jRelationship.getEndNodeId() + ",_type:" + neo4jRelationship.getType().toString() + ",_properties:" + mapper.writeValueAsString(neo4jRelationship.getAllProperties()) + "}";
            logObjectString = "relationship: " + neo4jRelationshipAsJson;
        } else {
            logObjectString = logObject.toString();
        }

        log.info("sc.log: " + logString.toString() + " " + logObjectString);
        return logObject;
    }

    @UserFunction
    @Description("RETURN sc.log.info('log info ')  // ")
    public Map<String, Object> infoNull(
            @Name("logString") Object logString,
            @Name(value = "logObject", defaultValue = "{}") Map<String, Object> logObject
    ) throws JsonProcessingException {
        String logObjectString = "";
        if (logObject instanceof Map) {
            ObjectMapper mapper = new ObjectMapper();
            logObjectString = mapper.writeValueAsString(logObject); //(String) logObject.toString();
        } else if (logObject instanceof Node) {
            ObjectMapper mapper = new ObjectMapper();
            Node neo4jNode = (Node) logObject;
            String neo4jNodeAsJson = "{_id:" + neo4jNode.getId() + ",_labels:" + neo4jNode.getLabels().toString() + ",_properties:" + mapper.writeValueAsString(neo4jNode.getAllProperties()) + "}";
            logObjectString = "node: " + neo4jNodeAsJson;
        } else if (logObject instanceof Relationship) {
            ObjectMapper mapper = new ObjectMapper();
            Relationship neo4jRelationship = (Relationship) logObject;
            String neo4jRelationshipAsJson = "{_id:" + neo4jRelationship.getId() + ",_from:" + neo4jRelationship.getStartNodeId() + ",_to:" + neo4jRelationship.getEndNodeId() + ",_type:" + neo4jRelationship.getType().toString() + ",_properties:" + mapper.writeValueAsString(neo4jRelationship.getAllProperties()) + "}";
            logObjectString = "relationship: " + neo4jRelationshipAsJson;
        } else {
            logObjectString = logObject.toString();
        }

        log.info("sc.log: " + logString.toString() + " " + logObjectString);
        return null;
    }

}
