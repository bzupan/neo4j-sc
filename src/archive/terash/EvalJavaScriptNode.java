package sc.javascript;

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.Map;

/**
 * This is an example how you can create a simple user-defined function for
 * Neo4j.
 */
public class EvalJavaScriptNode {

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.evalJavaScriptOnNode(\"function functionName(name) {return 'Hello ' + name}\",\"functionName\" ,\"functionParams\" )")
    public String evalJavaScriptOnNode(
            @Name("javaScriptString") String javaScriptString,
            @Name("functionName") String functionName,
            @Name("functionParams") String functionParams
    //@Name("node") Node node
    ) throws ScriptException, NoSuchMethodException, JsonProcessingException {
        //https://winterbe.com/posts/2014/04/05/java8-nashorn-tutorial/
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval(javaScriptString);
        Invocable invocable = (Invocable) engine;

        Object funcResult = invocable.invokeFunction(functionName, functionParams);
        log.info((String) funcResult);
        //log.info(result.getClass().toString());

        return funcResult.toString();

//        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
//        ScriptEngine nashorn = scriptEngineManager.getEngineByName("nashorn");
//
//        log.info("javaScriptEval javascriptString: " + javaScriptString);
//
//        String name = "Mahesh";
//        Integer result = null;
//        //String script =  'const Http = new XMLHttpRequest();const url="https://jsonplaceholder.typicode.com/posts";Http.open("GET", url);Http.send();Http.onreadystatechange=(e)=>{console.log(Http.responseText)}';
//
//        try {
//            nashorn.eval("print('" + name + "')");
//            result = (Integer) nashorn.eval(javaScriptString);
//
//        } catch (ScriptException e) {
//            System.out.println("Error executing script: " + e.getMessage());
//        }
//        return result.toString();
    }

}
