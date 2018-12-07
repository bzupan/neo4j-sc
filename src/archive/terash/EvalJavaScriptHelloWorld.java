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
    @Description("CALL example.javascriptString()")
    public String evalJavaScriptHelloWorld(
            @Name("javaScriptString") String javaScriptString
    ) throws ScriptException, NoSuchMethodException {
        //https://winterbe.com/posts/2014/04/05/java8-nashorn-tutorial/
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval("var fun1 = function(name) {\n"
                + "    return \"greetings from javascript\";\n"
                + "};\n"
                + "var fun2 = function (object) {\n"
                + "    print(\"JS Class Definition: \" + Object.prototype.toString.call(object));\n"
                + "};");

        Invocable invocable = (Invocable) engine;

        Object result = invocable.invokeFunction("fun1", "Peter Parker");
        log.info((String) result);
        log.info(result.getClass().toString());

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
