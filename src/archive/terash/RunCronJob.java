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
public class RunCronJob {

    @Context
    public Log log;

    @UserFunction
    @Description("RETURN sc.evalJavaScript(\" var properties = JSON.parse(stringObject);\\n ar replyString = 'Hello, ' + properties.name + '!';\\n return replyString;\\n \",{\"name\":\"aaa\"})")
    // RETURN sc.evalJavaScript(" var properties = JSON.parse(stringObject); var replyString = 'Hello, ' + properties.name + '!'; return replyString;","{\"name\":\"aaaaaaa\"}")
    public Object runCronJob(
            @Name("jsScript") String jsScript,
            @Name("jsParams") Object jsParams
    ) throws ScriptException, NoSuchMethodException {
        //https://winterbe.com/posts/2014/04/05/java8-nashorn-tutorial/
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

//        engine.eval(
//                "var javaScriptFunction = function(stringObject) {\n"
//                + "  var properties = JSON.parse(stringObject);\n"
//                + "  var replyString = 'Hello, ' + properties.name + '!';\n"
//                + "  return replyString;\n"
//                + "};");
                engine.eval(
                "var javaScriptFunction = function(jsParams) {\n"
                + jsScript
                + "};");

        Invocable invocable = (Invocable) engine;
        
       // Object result = invocable.invokeFunction("javaScriptFunction", "{\"name\":\"aaa\"}");
        Object result = invocable.invokeFunction("javaScriptFunction", jsParams);
        return result;

    }

}
