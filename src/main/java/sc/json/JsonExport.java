package sc.json;

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.Node;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;
import java.util.Map;
import org.neo4j.graphdb.Relationship;

/**
 * Export Neo4j node to JSON string
 *
 */
public class JsonExport {

    @Context
    public Log log;

    @UserFunction
    @Description("MATCH (n) WHERE ID(n)= 123 RETURN sc.json.jsonExport(n)  // return node map ralationshipas json string")
    public String jsonExport(
            @Name("neo4jNode") Object neo4jObject
    ) throws MalformedURLException,
            UnsupportedEncodingException,
            JsonProcessingException {

        log.debug("sc.jsonExport neo4jObject: " + neo4jObject.toString());
        String jsonString = "";
        if (neo4jObject instanceof Map) {
            ObjectMapper mapper = new ObjectMapper();
            jsonString = mapper.writeValueAsString(neo4jObject);
            log.debug("sc.jsonExport instanceof Map: " + jsonString.toString());
        } else if (neo4jObject instanceof Node) {
            ObjectMapper mapper = new ObjectMapper();
            Node neo4jNode = (Node) neo4jObject;
            jsonString = "{_id:" + neo4jNode.getId() + ",_labels:" + neo4jNode.getLabels().toString() + ",_properties:" + mapper.writeValueAsString(neo4jNode.getAllProperties()) + "}";
            log.debug("sc.jsonExport instanceof Node: " + jsonString.toString());
        } else if (neo4jObject instanceof Relationship) {
            ObjectMapper mapper = new ObjectMapper();
            Relationship neo4jRelationship = (Relationship) neo4jObject;
            jsonString = "{_id:" + neo4jRelationship.getId() + ",_from:" + neo4jRelationship.getStartNodeId() + ",_to:" + neo4jRelationship.getEndNodeId() + ",_type:" + neo4jRelationship.getType().toString() + ",_properties:" + mapper.writeValueAsString(neo4jRelationship.getAllProperties()) + "}";
            log.debug("sc.jsonExport instanceof Relationship: " + jsonString.toString());
        } else {
            jsonString = "{string:" +neo4jObject.toString() + "}";
            log.debug("sc.jsonExport instanceof Object: " + jsonString.toString());

        }
        log.debug("sc.jsonExport jsonString: " + jsonString);
        return jsonString;
    }

}
