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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

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
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.neo4j.graphdb.Relationship;

/**
 * This is an Neo4j node properties post .
 */
public class HttpPostClient {

    private static final HttpPostJson http = new HttpPostJson("insecure");

    @Context
    public Log log;

    @UserFunction
    @Description("sc.rest.httpPostJson(\"url string\", node) - return json post Response")
    public Map<String, Object> httpPostJson(
            @Name("url") String url,
            @Name("node") Object inputObject
    ) {
        Map<String, Object> reaponse = http.post(url, inputObject);
        return reaponse;
    }

    @UserFunction
    @Description("sc.rest.httpPostNode(\"url string\", node) - return json post")
    public Map<String, Object> httpPostNode(
            @Name("url") String url,
            @Name("node") Node node
    ) {
        // Map<String, Object> cypherNodeTmp = new HashMap<String, Object>(node.getAllProperties());
        // cypherNodeTmp.put("_id", node.getId());
        // cypherNodeTmp.put("_labels", node.getLabels());
        Map<String, Object> reaponse = http.post(url, node.getAllProperties());
        return reaponse;//content.toString();
    }

    @UserFunction
    @Description("sc.rest.httpPostRelationship(\"url string\", Relationship) - return json post")
    public Map<String, Object> httpPostRelationship(
            @Name("url") String url,
            @Name("relationship") Relationship relationship
    ) {
        // Map<String, Object> cypherNodeTmp = new HashMap<String, Object>(node.getAllProperties());
        // cypherNodeTmp.put("_id", node.getId());
        // cypherNodeTmp.put("_labels", node.getLabels());
        Map<String, Object> reaponse = http.post(url, relationship.getAllProperties());
        return reaponse;//content.toString();
    }

    // --- http https post
    public static class HttpPostJson {

        CloseableHttpClient httpClient;

        public HttpPostJson(String httpType) {
            if (httpType.equals("insecure")) {
                try {
                    final SSLContext sslContext = new SSLContextBuilder()
                            .loadTrustMaterial(null, (x509CertChain, authType) -> true)
                            .build();
                    httpClient = HttpClientBuilder.create()
                            .setSSLContext(sslContext)
                            .setConnectionManager(
                                    new PoolingHttpClientConnectionManager(
                                            RegistryBuilder.<ConnectionSocketFactory>create()
                                                    .register("http", PlainConnectionSocketFactory.INSTANCE)
                                                    .register("https", new SSLConnectionSocketFactory(sslContext,
                                                            NoopHostnameVerifier.INSTANCE))
                                                    .build()
                                    ))
                            .build();
                } catch (Exception ex) {
                    System.out.println(ex.toString());
                    httpClient = HttpClientBuilder.create().build();
                }
            } else {
                httpClient = HttpClientBuilder.create().build();
            }
        }

        public Map<String, Object> post(String url, Object inputObject) {
            Map<String, Object> map = new HashMap<String, Object>();
            try {
                //Object to JSON in String
                ObjectMapper mapper = new ObjectMapper();
                Gson gson = new Gson();

                //   log.info("httpJsonPost source: " + node.getId() + " " + node.getLabels().toString() + " " + node.getAllProperties().toString());
                String jsonInString = mapper.writeValueAsString(inputObject);
                System.out.println("httpJsonPost as json: " + jsonInString.toString());

                HttpPost postRequest = new HttpPost(url);

                postRequest.setEntity(new StringEntity(jsonInString));
                postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

                CloseableHttpResponse httpResponse = httpClient.execute(postRequest);
                String content = EntityUtils.toString(httpResponse.getEntity());
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                System.out.println("httpJsonPost statusCode = " + statusCode + " content = " + content);

                map = new Gson().fromJson(content.toString(), Map.class);

            } catch (JsonProcessingException ex) {
                System.out.println(ex.toString());
                map.put("error", "JsonProcessingException");
            } catch (UnsupportedEncodingException ex) {
                System.out.println(ex.toString());
                map.put("error", "UnsupportedEncodingException");
            } catch (IOException ex) {
                System.out.println(ex.toString());
                map.put("error", "IOException");
            } catch (Exception ex) {
                System.out.println(ex.toString());
                map.put("error", "Exception");
            }
            return map;
        }
    }
}
