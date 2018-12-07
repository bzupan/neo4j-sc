package sc.rest;

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

/**
 * url encode string.
 */
public class UrlEncodeString {

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.urlEncodeString('{\"jsonrpc\":\"2.0\",\"method\":\"pingIpAddress\",\"params\":{\"ipAddress\":\"10.10.103.254\",\"pingCount\":2,\"pingInterval\":1,\"pingPacketSize\":56,\"timeoutSec\":20},\"id\":null}') - return  url encoded string")
    public String urlEncodeString(
            @Name("strings") String stringUrl
    ) throws MalformedURLException,
            UnsupportedEncodingException {
        log.debug("sc.urlEncodeString source: " + stringUrl);

        String stringUrlEncoded = URLEncoder.encode(stringUrl, "UTF-8");

        log.debug("sc.urlEncodeString encoded: " + stringUrlEncoded);

        return stringUrlEncoded;
    }
}
