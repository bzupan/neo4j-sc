package sc.rest;

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
import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * This is an Neo4j node properties post example Neo4j.
 */
public class HttpPostNode {

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @UserFunction
    @Description("sc.httpPostNode(\"url string\", node) - return json post")
    public Map<String, String> httpPostNode(
            @Name("url") String url,
            @Name("node") Node node
    ) throws MalformedURLException,
            UnsupportedEncodingException,
            JsonProcessingException,
            IOException {

        //Object to JSON in String
        ObjectMapper mapper = new ObjectMapper();
        Gson gson = new Gson();

        log.info("httpJsonPost source: " + node.getId() + " " + node.getLabels().toString() + " " + node.getAllProperties().toString());

        String nodeAsJson = "{_id:" + node.getId() + ",_labels:" + node.getLabels().toString() + "{_properties:" + mapper.writeValueAsString(node.getAllProperties()) + "}";

        String jsonInString = mapper.writeValueAsString(node.getAllProperties());
        log.info("httpJsonPost as json: " + jsonInString.toString());

        // rest json
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost postRequest = new HttpPost(url);

        postRequest.setEntity(new StringEntity(jsonInString));
        postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

   
        CloseableHttpResponse httpResponse = httpClient.execute(postRequest);
        String content = EntityUtils.toString(httpResponse.getEntity());
        int statusCode = httpResponse.getStatusLine().getStatusCode();
        log.info("httpJsonPost statusCode = " + statusCode + " content = " + content);

        Map<String, String> map = new Gson().fromJson(content.toString(), Map.class);
        return map;//content.toString();
        
    }
}
