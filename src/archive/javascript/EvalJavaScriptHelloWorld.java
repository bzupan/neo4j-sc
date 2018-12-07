package sc;

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

/**
 * This is an example how you can create a simple user-defined function for
 * Neo4j.
 */
public class EvalJavaScriptHelloWorld {

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.evalJavaScriptHelloWorld")
    public String evalJavaScriptHelloWorld(
            @Name("javaScriptString") String javaScriptString
    ) throws ScriptException, NoSuchMethodException {
        //https://winterbe.com/posts/2014/04/05/java8-nashorn-tutorial/
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

        engine.eval(
                "var sayHello = function(name) {\n"
                + "  var replyString = 'Hello, ' + name + '!';\n"
                + "  return replyString;\n"
                + "};"
                + "var functionObject = function(stringObject) {\n"
                + "  var properties = JSON.parse(stringObject);\n"
                + "  var replyString = 'Hello, ' + properties.name + '!';\n"
                + "  return replyString;\n"
                + "};");

// cast the script engine to an invocable instance
        Invocable invocable = (Invocable) engine;

        //Object result = invocable.invokeFunction("sayHello", "John Doe");
         Object result = invocable.invokeFunction("functionObject", "{\"name\":\"aaa\"}");
        return result.toString();

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
