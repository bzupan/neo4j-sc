package sc;

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
 * This is an example how you can create a simple user-defined function for
 * Neo4j.
 */
public class JsonEncodedNode {

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @UserFunction
    @Description("MATCH (n) WHERE ID(n)= 123 RETURN example.jsonEncodedNode(n) - return  json encoded node")
    public String jsonEncodedNode(
            @Name("value") Node value
    ) throws MalformedURLException,
            UnsupportedEncodingException,
            JsonProcessingException {
        //String hello = "Hello World!" + " " + stringUrl;
        //Object to JSON in String
        ObjectMapper mapper = new ObjectMapper();
        //  Gson gson = new Gson();

        log.info("urlEncodeNode source: " + value.getId() + " " + value.getLabels().toString() + " " + value.getAllProperties().toString());

        String nodeAsJson = "{_id:" + value.getId() + ",_labels:" + value.getLabels().toString() + "{_properties:" + mapper.writeValueAsString(value.getAllProperties()) + "}";
        String jsonInString = mapper.writeValueAsString(value.getAllProperties());
        log.info("urlEncodeNode as json: " + jsonInString.toString());
//        //String objectString = jsonInString.toString();
        //     String stringUrlEncoded = URLEncoder.encode(jsonInString, "UTF-8");
        //       log.info("urlEncodeNode encoded: " + stringUrlEncoded);
//       return stringUrlEncoded;

//        String json = gson.toJson(value.toString());
//             log.info("urlEncode encoded: " + json);
        return nodeAsJson.toString();
    }
}
