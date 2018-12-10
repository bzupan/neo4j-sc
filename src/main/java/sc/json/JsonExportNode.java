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

/**
 * Export Neo4j node to JSON string
 *
 */
public class JsonExportNode {

    @Context
    public Log log;

    @UserFunction
    @Description("MATCH (n) WHERE ID(n)= 123 RETURN sc.jsonExportNode(n)  - return node as json string")
    public String jsonExportNode(
            @Name("neo4jNode") Node neo4jNode
    ) throws MalformedURLException,
            UnsupportedEncodingException,
            JsonProcessingException {

        // --- Object to JSON in String
        ObjectMapper mapper = new ObjectMapper();

        log.debug("sc.jsonExportNode neo4jNode: " + neo4jNode.getId() + " " + neo4jNode.getLabels().toString() + " " + neo4jNode.getAllProperties().toString());

        String neo4jNodeAsJson = "{_id:" + neo4jNode.getId() + ",_labels:" + neo4jNode.getLabels().toString() + ",_properties:" + mapper.writeValueAsString(neo4jNode.getAllProperties()) + "}";

        log.debug("sc.jsonExportNode neo4jNodeAsJson: " + neo4jNodeAsJson);

        return neo4jNodeAsJson;
    }
}
