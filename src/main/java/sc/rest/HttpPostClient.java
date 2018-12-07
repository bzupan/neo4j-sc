package sc.rest;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;

import java.io.UnsupportedEncodingException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

/**
 * post from Neo4j
 */
public class HttpPostClient {

    private static final HttpPostJson http = new HttpPostJson();
    private static final String httpClientProperties = "{enableInsecureHttps: false, enableErrorMessage:false}";

    @Context
    public Log log;

    @UserFunction
    @Description(
            "// - return json post response\n"
            + "RETURN sc.rest.httpPostJson(\"https://127.0.0.1:8443/restTest\", {jsonObject:123},{enableInsecureHttps: false, enableErrorMessage:false}) AS postResponse "
    )
    public Map<String, Object> httpPostJson(
            @Name("url") String url,
            @Name("node") Object inputObject,
            @Name(value = "httpClientOptions", defaultValue = httpClientProperties) Map<String, Object> httpClientOptions
    ) {
        log.debug("sc.rest.httpPostJson - input: " + url.toString() + " " + inputObject.toString() + " " + httpClientOptions.toString());
        Map<String, Object> reaponse = http.post(url, inputObject, httpClientOptions);
        log.debug("sc.rest.httpPostJson - reaponse: " + url.toString() + " " + inputObject.toString() + " " + httpClientOptions.toString());
        return reaponse;
    }

    @UserFunction
    @Description(
            "// - return node post response\n"
            + "MATCH (n:scTestNode) WHERE ID(n)=434\n"
            + "RETURN sc.rest.httpPostNode(\n"
            + "\"http://127.0.0.1/restTest\", \n"
            + "n,\n"
            + "{enableInsecureHttps: false, enableErrorMessage:false}\n"
            + ") \n"
            + "AS postResponseNode"
    )
    public Map<String, Object> httpPostNode(
            @Name("url") String url,
            @Name("node") Node node,
            @Name(value = "httpClientOptions", defaultValue = httpClientProperties) Map<String, Object> httpClientOptions
    ) {
        log.debug("sc.rest.httpPostNode - input: " + url.toString() + " " + node.toString() + " " + httpClientOptions.toString());
        Map<String, Object> reaponse = http.post(url, node.getAllProperties(), httpClientOptions);
        log.debug("sc.rest.httpPostNode - reaponse: " + url.toString() + " " + node.toString() + " " + httpClientOptions.toString());
        return reaponse;
    }

    @UserFunction
    @Description("\n" +
"            \"// - return node post response\\n\"\n" +
"            + \"MATCH ()-[l]->()HERE ID(l)=434\\n\"\n" +
"            + \"RETURN sc.rest.httpPostNode(\\n\"\n" +
"            + \"\\\"http://127.0.0.1/restTest\\\", \\n\"\n" +
"            + \"n,\\n\"\n" +
"            + \"{enableInsecureHttps: false, enableErrorMessage:false}\\n\"\n" +
"            + \") \\n\"\n" +
"            + \"AS postResponseNode\"\n" +
"    ")
    public Map<String, Object> httpPostRelationship(
            @Name("url") String url,
            @Name("relationship") Relationship relationship,
            @Name(value = "httpClientOptions", defaultValue = httpClientProperties) Map<String, Object> httpClientOptions
    ) {
        log.debug("sc.rest.httpPostRelationship - input: " + url.toString() + " " + relationship.toString() + " " + httpClientOptions.toString());
        Map<String, Object> reaponse = http.post(url, relationship.getAllProperties(), httpClientOptions);
        log.debug("sc.rest.httpPostRelationship - reaponse: " + url.toString() + " " + relationship.toString() + " " + httpClientOptions.toString());
        return reaponse;
    }

    // --- http https post
    public static class HttpPostJson {

        CloseableHttpClient httpClientSecure;
        CloseableHttpClient httpClientInsecure;

        public HttpPostJson() {
            try {
                // --- httpClient insecure
                final SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, (x509CertChain, authType) -> true)
                        .build();
                httpClientInsecure = HttpClientBuilder.create()
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
                System.out.println("sc.rest.httpPostRelationship - error: " + ex.toString());
            }

            // --- httpClient secure
            httpClientSecure = HttpClientBuilder.create().build();
        }

        public Map<String, Object> post(String url, Object inputObject, Map<String, Object> httpClientOptions) {

            CloseableHttpClient httpClient;

            // -- check for client type
            if (httpClientOptions.get("enableInsecureHttps").equals(false)) {
                httpClient = httpClientSecure;
            } else {
                httpClient = httpClientInsecure;
            }

            // --- result variables
            Map<String, Object> httpClientResultMap = new HashMap<String, Object>();
            Boolean httpClientError = false;

            // --- http post
            try {
                // --- Object to JSON 
                ObjectMapper mapper = new ObjectMapper();
                String jsonInString = mapper.writeValueAsString(inputObject);

                // --- post
                HttpPost postRequest = new HttpPost(url);
                postRequest.setEntity(new StringEntity(jsonInString));
                postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
                CloseableHttpResponse httpResponse = httpClient.execute(postRequest);

                // --- response
                String content = EntityUtils.toString(httpResponse.getEntity());
                int statusCode = httpResponse.getStatusLine().getStatusCode();

                httpClientResultMap = new Gson().fromJson(content.toString(), Map.class);
                httpClientError = false;

            } catch (JsonProcessingException ex) {
                System.out.println("sc.rest.httpPostRelationship - error: JsonProcessingException " + ex.toString());
                httpClientResultMap.put("error", "JsonProcessingException");
                httpClientError = true;
            } catch (UnsupportedEncodingException ex) {
                System.out.println("sc.rest.httpPostRelationship - error: UnsupportedEncodingException " + ex.toString());
                httpClientResultMap.put("error", "UnsupportedEncodingException");
                httpClientError = true;
            } catch (IOException ex) {
                System.out.println("sc.rest.httpPostRelationship - error: IOException" + ex.toString());
                httpClientResultMap.put("error", "IOException");
                httpClientError = true;
            } catch (Exception ex) {
                System.out.println("sc.rest.httpPostRelationship - error: " + ex.toString());
                httpClientResultMap.put("error", "Exception");
                httpClientError = true;
            }

            if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                // --- send result or error message 
                return httpClientResultMap;
            } else {
                // --- send null on error 
                if (httpClientError.equals(true)) {
                    // --- send null on error
                    return null;
                } else {
                    // --- return result
                    return httpClientResultMap;
                }
            }
        }
    }
}
