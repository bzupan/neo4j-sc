

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

/**
 * This is an example how you can create a simple user-defined function for
 * Neo4j.
 */
public class Neo4jUserFunctionSnipet {

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @UserFunction
    @Description("sc.scHello('string') - return message and print to log")
    public String scHello(
            @Name("strings") String string
    ) {
        String hello = "Hello World!" + " " + string;
        log.info("sc.scHello: " + hello);
        System.out.println("scHello: " + hello);
        return hello;
    }
}
