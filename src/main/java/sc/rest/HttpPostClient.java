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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
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
            "// - return node post response "
            + "MATCH MATCH ()-[l]->()HERE ID(l)=434 "
            + "RETURN sc.rest.jsonPost( "
            + "\"http://127.0.0.1/restTest\",  "
            + "l "
            + "{enableInsecureHttps: false, enableErrorMessage:false} "
            + ")  n"
            + "AS postResponseNode"
    )
    public Map<String, Object> jsonPost(
            @Name("url") String url,
            @Name("params") Object params,
            @Name(value = "httpClientOptions", defaultValue = httpClientProperties) Map<String, Object> httpClientOptions
    ) {
        // --- prepere rpc request input
        Map<String, Object> jsonResponse = null;
        Map<String, Object> jsonParams = new HashMap();

        if (params instanceof Map) {
            jsonParams = (Map<String, Object>) params;
        } else if (params instanceof Node) {
            jsonParams = (Map<String, Object>) ((Node) params).getAllProperties();
        } else if (params instanceof Relationship) {
            jsonParams = (Map<String, Object>) ((Relationship) params).getAllProperties();
        } else {
            if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                jsonResponse = new HashMap();
                jsonResponse.put("error", "http post request error - wrong request Map, Node or Relationship expected - got: " + params);
                return (Map<String, Object>) jsonResponse;
            } else {
                return null;
            }

        }

        try {
            jsonResponse = http.postTimeout(url, jsonParams, httpClientOptions);
            log.debug("sc.rest.jsonPost - input: " + url.toString() + " " + jsonParams.toString() + " " + httpClientOptions.toString() + " " + jsonResponse.toString());
            return (Map<String, Object>) jsonResponse;

        } catch (Exception ex) {
            log.error("sc.rest.jsonPost - input: " + url.toString() + " " + jsonParams.toString() + " " + httpClientOptions.toString() + " " + ex.toString());
            if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                jsonResponse = new HashMap();
                jsonResponse.put("error", "http post error: " + ex.toString());
                return (Map<String, Object>) jsonResponse;
            } else {
                return null;
            }
        }
    }

    //   RETURN sc.rest.jsonRpc2('https://10.20.36.110:10443/jsonRpc2RestPost', 'pingIpAddress', {ipAddress:'8.8.8.8'},{enableInsecureHttps: true, enableErrorMessage:true}) AS postResponse
    // RETURN sc.rest.jsonRpc2('https://10.20.36.110:10443/jsonRpc2RestPost', 'scanIpNetwork', {ipNetwork:'10.20.36.0/24'},{enableInsecureHttps: true, enableErrorMessage:true}) AS postResponse
    @UserFunction
    @Description(
            "// - return json post response "
            + "RETURN sc.rest.jsonRpc2('https://10.20.36.110:10443/jsonRpc2RestPost', 'pingIpAddress', {ipAddress:'8.8.8.8'},{enableInsecureHttps: true, enableErrorMessage:true}) AS postResponse"
    )

    public Object jsonRpc2(
            @Name("jsonRpc2Url") String jsonRpc2Url,
            @Name("method") String method,
            @Name("params") Object params,
            @Name(value = "httpClientOptions", defaultValue = httpClientProperties) Map<String, Object> httpClientOptions
    ) {
        // --- prepere rpc request input
        Map<String, Object> jsonRpc2Request = new HashMap();
        Map<String, Object> jsonRpc2response = null;
        Random jsonRpc2Id = new Random();
        Map<String, Object> jsonRpc2Params = new HashMap();

        if (params instanceof Map) {
            jsonRpc2Params = (Map<String, Object>) params;
        } else if (params instanceof Node) {
            jsonRpc2Params = (Map<String, Object>) ((Node) params).getAllProperties();
        } else if (params instanceof Relationship) {
            jsonRpc2Params = (Map<String, Object>) ((Relationship) params).getAllProperties();
        } else {
            if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                jsonRpc2response = new HashMap();
                jsonRpc2response.put("error", "http post request error - wrong request Map, Node or Relationship expected - got: " + params);
                return (Map<String, Object>) jsonRpc2response;
            } else {
                return null;
            }

        }

        jsonRpc2Request.put("jsonrpc", "2.0");
        jsonRpc2Request.put("method", method);
        jsonRpc2Request.put("params", jsonRpc2Params);
        jsonRpc2Request.put("id", jsonRpc2Id.nextInt());

        try {
            jsonRpc2response = http.postTimeout(jsonRpc2Url, jsonRpc2Request, httpClientOptions);
            log.debug("sc.rest.jsonRpc2 - input: " + jsonRpc2Url.toString() + " " + jsonRpc2Request.toString() + " " + httpClientOptions.toString() + " " + jsonRpc2response.toString());

            if (!(jsonRpc2response.get("result") == null)) {
                Object jsonRpc2responseResult = jsonRpc2response.get("result");
                if (jsonRpc2responseResult instanceof Map) {
                    return (Map<String, Object>) jsonRpc2responseResult;
                } else if (jsonRpc2responseResult instanceof ArrayList) {
                    return (List<Map<String, Object>>) jsonRpc2responseResult;
                } else {
                    return jsonRpc2responseResult;
                }

            } else {
                log.error("sc.rest.jsonRpc2 response error: " + jsonRpc2Url.toString() + " " + jsonRpc2Request.toString() + " " + httpClientOptions.toString() + " " + jsonRpc2response.toString());
                if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                    return (Map<String, Object>) jsonRpc2response;
                } else {
                    return null;
                }
            }

        } catch (Exception ex) {
            log.error("sc.rest.jsonRpc2 http error: " + jsonRpc2Url.toString() + " " + jsonRpc2Request.toString() + " " + httpClientOptions.toString() + " " + ex.toString());
            if (httpClientOptions.get("enableErrorMessage").equals(true)) {
                jsonRpc2response = new HashMap();
                jsonRpc2response.put("error", "http post error: " + ex.toString());
                return (Map<String, Object>) jsonRpc2response;
            } else {
                return null;
            }
        }
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
                        .setMaxConnTotal(20)
                        .setMaxConnPerRoute(20)
                        .setConnectionTimeToLive(300000, TimeUnit.MILLISECONDS)
                        .build();
            } catch (Exception ex) {
                System.out.println("sc.rest.httpPostRelationship - error: " + ex.toString());
            }

            // --- httpClient secure
            httpClientSecure = HttpClientBuilder
                    .create()
                    .setConnectionTimeToLive(300000, TimeUnit.MILLISECONDS)
                    .build();

//            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
//// Increase max total connection to 200
//cm.setMaxTotal(200);
//// Increase default max connection per route to 20
//cm.setDefaultMaxPerRoute(20);
//
//// Increase max connections for localhost:80 to 50
//HttpHost localhost = new HttpHost("locahost", 80);
//cm.setMaxPerRoute(new HttpRoute(localhost), 50);
//
//CloseableHttpClient httpClient = HttpClients.custom()
//        .setConnectionManager(cm)
//        .build();
        }

        public Map<String, Object> post(String url, Object inputObject, Map<String, Object> httpClientOptions) throws JsonProcessingException, UnsupportedEncodingException, IOException {

            CloseableHttpClient httpClient;

            // -- check for client type
            if (httpClientOptions.get("enableInsecureHttps").equals(false)) {
                httpClient = httpClientSecure;
            } else {
                httpClient = httpClientInsecure;
            }

            // --- result variables
            Map<String, Object> httpClientResultMap = new HashMap<String, Object>();

            // --- http post
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

            // --- return result
            return httpClientResultMap;

        }

        public Map<String, Object> postTimeout(String url, Object inputObject, Map<String, Object> httpClientOptions) throws JsonProcessingException, UnsupportedEncodingException, IOException {
            int timeoutSec = 5;
            if (httpClientOptions.get("timeoutSec") != null) {
                long timeoutSecLong = (long) httpClientOptions.get("timeoutSec");
                timeoutSec = (int) (long) timeoutSecLong;
            }
            //System.out.print("timeoutSec" +timeoutSec);
            RequestConfig requestConfig = RequestConfig.custom()
                    // Determines the timeout in milliseconds until a connection is established.
                    .setConnectTimeout(timeoutSec * 1000)
                    // Defines the socket timeout in milliseconds,
                    // which is the timeout for waiting for data or, put differently,
                    // a maximum period inactivity between two consecutive data packets).
                    .setSocketTimeout(timeoutSec * 1000)
                    // Returns the timeout in milliseconds used when requesting a connection
                    // from the connection manager.
                    .setConnectionRequestTimeout(timeoutSec * 1000)
                    .build();

            CloseableHttpClient httpClient;

            // -- check for client type
            if (httpClientOptions.get("enableInsecureHttps").equals(false)) {
                httpClient = httpClientSecure;
            } else {
                httpClient = httpClientInsecure;
            }

            // --- result variables
            Map<String, Object> httpClientResultMap = new HashMap<String, Object>();

            // --- http post
            // --- Object to JSON
            ObjectMapper mapper = new ObjectMapper();
            String jsonInString = mapper.writeValueAsString(inputObject);

            // --- post
            HttpPost postRequest = new HttpPost(url);
            postRequest.setEntity(new StringEntity(jsonInString));
            postRequest.setConfig(requestConfig);
            postRequest.setHeader(HttpHeaders.CONNECTION, "close");
            postRequest.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            CloseableHttpResponse httpResponse = httpClient.execute(postRequest);

            // --- response
            String content = EntityUtils.toString(httpResponse.getEntity());
            int statusCode = httpResponse.getStatusLine().getStatusCode();

            httpClientResultMap = new Gson().fromJson(content.toString(), Map.class);

            // --- return result
            return httpClientResultMap;

        }

    }
}
