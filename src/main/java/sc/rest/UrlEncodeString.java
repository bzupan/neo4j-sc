package sc.rest;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;

/**
 * url encode string
 */
public class UrlEncodeString {

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.rest.urlEncodeString('aaa.bbb@email.com {jsonString:\"!\"#$&/()\"}') AS urlEncodeString) // return  url encoded string")
    public String urlEncodeString(
            @Name("stringUrl") String stringUrl
    ) throws MalformedURLException,
            UnsupportedEncodingException {
        log.debug("sc.rest.urlEncodeString - input: " + stringUrl);
        String stringUrlEncoded = URLEncoder.encode(stringUrl, "UTF-8");
        log.debug("sc.rest.urlEncodeString - encoded: " + stringUrlEncoded);
        return stringUrlEncoded;
    }
}
