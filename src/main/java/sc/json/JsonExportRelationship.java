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
import java.net.URL;
import java.net.URLEncoder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;
import org.neo4j.graphdb.Relationship;

/**
 * Export Neo4j relationship to JSON string
 *
 */
public class JsonExportRelationship {

    @Context
    public Log log;

    @UserFunction
    @Description("MATCH ()-[r]-() WHERE ID(r)= 123 RETURN sc.JsonExportRelationship(r)  - return relationship as json string")
    public String jsonExportRelationship(
            @Name("neo4jRelationship") Relationship neo4jRelationship
    ) throws MalformedURLException,
            UnsupportedEncodingException,
            JsonProcessingException {
        // --- Object to JSON in String
        ObjectMapper mapper = new ObjectMapper();

        String neo4jRelationshipAsJson = "{_id:" + neo4jRelationship.getId() + ",_from:" + neo4jRelationship.getStartNodeId() + ",_to:" + neo4jRelationship.getEndNodeId() + ",_type:" + neo4jRelationship.getType().toString() + ",_properties:" + mapper.writeValueAsString(neo4jRelationship.getAllProperties()) + "}";

        log.debug("sc.jsonExportRelationship neo4jRelationshipAsJson: " + neo4jRelationshipAsJson);

        return neo4jRelationshipAsJson;
    }
}
