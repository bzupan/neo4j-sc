package sc.mqtt;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import sc.mqtt.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import sc.MapResult;

public class CypherMqtt {

    //private static final List< CypherMqtt.CypherQueryObject> cypherMqttList = new ArrayList<CypherQuery.CypherQueryObject>();
    private static final ArrayList<Map<String, Object>> cypherMqttList = new ArrayList<Map<String, Object>>();
    private static final Map<String, Object> mqttBrokersMap = new HashMap<String, Object>();
    
    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.listVm() - list all CYPHER java VM calls")
    public List< Map<String, Object>> list() {
        return cypherMqttList; //.map(CronJob::new);
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.addVm('name', 'cypher query', cypher query parameters') - add CYPHER Java VM calls")
    public Map<String, Object> add(
            @Name("name") String name,
            @Name("mqtt") Map<String, Object> mqtt
    ) {
        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherMqttList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            cypherQueryMap.put("mqtt", mqtt);
            //cypherQueryMap.put("parameters", name);
        } else {
            cypherQueryMap = new HashMap<String, Object>();
            log.info(cypherQueryMap.toString() + " create " + name);

            cypherQueryMap.put("name", name);
            cypherQueryMap.put("mqtt", mqtt);

            //cypherQueryMap.put("parameters", name);
            cypherMqttList.add(cypherQueryMap);

            String brokerUrl = mqtt.get("brokerUrl").toString();
            String clientId = mqtt.get("clientId").toString();;
            MemoryPersistence persistence = new MemoryPersistence();
            String topic = mqtt.get("topic").toString();;
            
            MqttClientNeo mqttBrokerNeo4jClient = new MqttClientNeo(brokerUrl, clientId, persistence);
            HashMap<String, Object> mqttBrokerTmp = new HashMap<String, Object>();
            mqttBrokerTmp.put("name", name);
            mqttBrokerTmp.put("mqtt", mqtt);
            mqttBrokerTmp.put("mqttBrokerNeo4jClient", mqttBrokerNeo4jClient);
            mqttBrokersMap.put(name, mqttBrokerTmp);

        }
        return cypherQueryMap;
    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.deleteVm('name') - add CYPHER Java VM calls")
    public Map<String, Object> delete(
            @Name("name") String name
    ) {
        Map<String, Object> mapNode = getMapFromListtByKeyValue(cypherMqttList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (mapNode != null) {
            log.info(mapNode.toString() + " remove " + mapNode.get("index"));
            cypherMqttList.remove(mapNode);
        }
        return null;
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    /**
     *
     * ProcessMqttMessage printMessage = new ProcessMqttMessage();
     * rectOne.listen(topic, printMessage);
     *
     */
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.runVm(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Stream<MapResult> publish(
            @Name("name") String name,
            @Name("query") String query,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherMqttList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            // --- run cypher query string
            String cypherFunctionQuery = cypherQueryMap.get("query").toString();

            log.info("runCypherNode - cypherFunctionQuery : " + cypherFunctionQuery);

            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
                tx2.success();
                return dbResult.stream().map(MapResult::new);
            }

        } else {
            return null;
        }
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.runVm(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Stream<MapResult> subscribe(
            @Name("name") String name,
            @Name("query") String query,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherMqttList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            // --- run cypher query string
            String cypherFunctionQuery = cypherQueryMap.get("query").toString();

            log.info("runCypherNode - cypherFunctionQuery : " + cypherFunctionQuery);

            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
                tx2.success();
                return dbResult.stream().map(MapResult::new);
            }

        } else {
            return null;
        }
    }

    // ----------------------------------------------------------------------------------
    // util
    // ----------------------------------------------------------------------------------
    private static Map<String, Object> getMapFromListtByKeyValue(final ArrayList<Map<String, Object>> inputArray, String key, String value) {
        Map<String, Object> cypherNodeTmp = null; //new HashMap<String, Object>();

        for (int i = 0; i < inputArray.size(); ++i) {
            System.out.println(key + " " + value);
            Map<String, Object> arrElement = inputArray.get(i);
            if (arrElement != null) {
                if (arrElement.get(key) != null) {
                    String currentKeyValue = arrElement.get(key).toString();
                    //   arrElement
                    //System.out.println(key + i);
                    //System.out.println(inputArray.get(i).toString());
                    System.out.println(" - " + currentKeyValue + " " + value);
                    if (currentKeyValue.equals(value)) {
                        System.out.println(" - -----------------------");
                        cypherNodeTmp = arrElement;
                        //cypherNodeTmp = new HashMap<String, Object>(arrElement);
                        //cypherNodeTmp.put("index", i);
                    } else {
                        System.out.println(" - -");
                    }
                }
            }

//            Object arrElement = inputArray[i];
//          if (inputArray[i] ===)
        }
        return cypherNodeTmp;

    }

    public class CypherQueryObject { // static

        public final String name;
        public String query;
        //public Map<String, Object> parameters;

        public CypherQueryObject(String cn, String cq) {
            this.name = cn;
            this.query = cq;
            //this.parameters = {};
        }

        public CypherQueryObject(String cs) {
            this.name = cs;
            //   this.cronScheduler = new Scheduler();
        }

        public CypherQueryObject addCron() {
            return this;
        }

        public CypherQueryObject getCron() {
            return this;
        }

        public CypherQueryObject update(String cs, String cn, String cq, boolean ce) {
            //this.cronString = cs;
            //this.cronName = cn;
            //this.cypherQuery = cq;
            //this.cronEnabled = ce;

            //this.cronScheduler.stop();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof CypherQueryObject && name.equals(((CypherQueryObject) o).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

    }

    /**
     * JSONUtils checkJson = new JSONUtils();
     * System.out.print(checkJson.jsonStringToMap(validJson));
     */
    public final static class JSONUtils {

        private final Gson gson = new Gson();

        private JSONUtils() {
        }

        public Object jsonStringToMap(String jsonInString) {
            try {
                Map<String, Object> retMap = new Gson().fromJson(jsonInString.toString(), new TypeToken<HashMap<String, Object>>() {
                }.getType());
                //gson.fromJson(jsonInString, Object.class);
                return retMap;
            } catch (JsonSyntaxException ex) {
                //System.out.println("This is finally block");
                return null;
            }
        }
    }

    public static class ProcessMqttMessage {

        public ProcessMqttMessage() {
        }

        public void run(Object message) {
            JSONUtils checkJson = new JSONUtils();
            System.out.println("Message: received " + message.toString());
            System.out.print(checkJson.jsonStringToMap(message.toString()));
        }
    }

    /**
     * public static void main(String[] args) { String broker =
     * "tcp://iot.eclipse.org:1883"; String clientId = "Neo4j-01";
     * MemoryPersistence persistence = new MemoryPersistence();
     *
     * MqttClientNeo rectOne = new MqttClientNeo(broker, clientId, persistence);
     *
     * String topic = "neo4j-string"; String content = "Message from
     * MqttPublishSample"; ProcessMqttMessage printMessage = new
     * ProcessMqttMessage(); rectOne.listen(topic, printMessage);
     * rectOne.publish(topic, content);
     *
     * }
     */
    public static class MqttClientNeo {

        int qos = 2;
        MqttClient sampleClient;

        // four constructors
        public MqttClientNeo(String broker, String clientId, MemoryPersistence persistence) {
            try {
                sampleClient = new MqttClient(broker, clientId, persistence);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                System.out.println("Connecting to broker: " + broker);
                sampleClient.connect(connOpts);
                //  brokerInt = broker;
            } catch (MqttException ex) {
                Logger.getLogger(CypherMqtt.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        private void publish(String topic, String content) {
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);

            System.out.println("Publishing message: " + content);
            try {
                sampleClient.publish(topic, message);
            } catch (MqttException ex) {
                Logger.getLogger(CypherMqtt.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Message published");
        }

        private void unsubscribe() {
            try {
                sampleClient.unsubscribe("#");
            } catch (MqttException ex) {
                Logger.getLogger(CypherMqtt.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("unsubscribe");
        }

        private void disconnect() {
            try {
                sampleClient.disconnect();
            } catch (MqttException ex) {
                Logger.getLogger(CypherMqtt.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("unsubscribe");
        }

        public void listen(String topic, ProcessMqttMessage task) {
            try {
                sampleClient.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) {
                    }

                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        task.run(message.toString());
                    }

                    public void deliveryComplete(IMqttDeliveryToken token) {
                    }
                });
                sampleClient.subscribe(topic);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }

    }

}


/*
    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.runFunctionVm(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Java VM)")
    public Object runFunctionVm(
            @Name("name") String name,
            @Name("params") Map<String, Object> params
    ) {
        // --- find node with cypher query from node name
        String cypherFunctionQuery = "";

        log.info("runCypherNode - cypherFunctionQuery : " + name);

        Map<String, Object> cypherQueryMap = getMapFromListtByKeyValue(cypherMqttList, "name", name);
        //log.info(mapNode.toString()+ " " + mapNode.get("index"));
        if (cypherQueryMap != null) {
            log.info(cypherQueryMap.toString() + " update " + name);
            // --- run cypher query string
            cypherFunctionQuery = cypherQueryMap.get("query").toString();
            Map<String, Object> cypherResult = new HashMap<String, Object>();
            try (Transaction tx2 = db.beginTx()) {
                Result dbResult = db.execute(cypherFunctionQuery, params);
                log.info("runCypherNode - cypher query: " + cypherFunctionQuery + ", " + params);

                // --- ger first row
                Map<String, Object> row = dbResult.next();
                for (Entry<String, Object> column : row.entrySet()) {
                    log.info("header" + column.getKey());
                    log.info("data" + column.getValue().toString());
                    List<Object> data = new ArrayList<Object>();
                    data.add(column.getValue());
                    cypherResult.put(column.getKey(), data);
                }

                // --- process rest of the rows
                while (dbResult.hasNext()) {
                    Map<String, Object> rowNext = dbResult.next();
                    for (Entry<String, Object> column : rowNext.entrySet()) {
                        log.info("header" + column.getKey().toString());
                        log.info("data" + column.getValue().toString());
                        List<Object> data = new ArrayList<Object>();
                        data = (List<Object>) cypherResult.get(column.getKey());
                        data.add(column.getValue());
                        cypherResult.put(column.getKey(), data);
                    }
                }
                tx2.success();
                return cypherResult;
            }
        } else {
            return null;
        }

    }

    // ----------------------------------------------------------------------------------
    // run function
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.runCypherNode(\"stringFunctionName\", {object:\"params\"} - run CYPHER from Neo4j DB)")
    public Object runFunctionDb(
            @Name("cypherFunctionName") String cypherFunctionName,
            @Name("cypherFunctionParameters") Map<String, Object> cypherFunctionParameters
    ) {
        // --- find node with cypher query from node name
        String cypherQueryString = "MATCH (n:CypherRunMqtt) WHERE n.name=\"" + cypherFunctionName + "\" RETURN n.query";

        log.info("runCypherNode - cypherFunctionQuery : " + cypherQueryString);

        // --- get cypher query string
        String cypherFunctionQuery = "null";
        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(cypherQueryString);

            // --- some debuging
            String column = result.columns().get(0);
            Map<String, Object> row1 = result.next();
            cypherFunctionQuery = row1.get(column).toString();

            
            tx.success();
        }

        // --- run cypher query string
        Map<String, Object> cypherResult = new HashMap<String, Object>();
        try (Transaction tx2 = db.beginTx()) {
            Result dbResult = db.execute(cypherFunctionQuery, cypherFunctionParameters);
            log.info("runCypherNode - cypher query: " + cypherFunctionQuery + ", " + cypherFunctionParameters);

            // --- ger first row
            Map<String, Object> row = dbResult.next();
            for (Entry<String, Object> column : row.entrySet()) {
                log.info("header" + column.getKey());
                log.info("data" + column.getValue().toString());
                List<Object> data = new ArrayList<Object>();
                data.add(column.getValue());
                cypherResult.put(column.getKey(), data);
            }

            // --- process rest of the rows
            while (dbResult.hasNext()) {
                Map<String, Object> rowNext = dbResult.next();
                for (Entry<String, Object> column : rowNext.entrySet()) {
                    log.info("header" + column.getKey().toString());
                    log.info("data" + column.getValue().toString());
                    List<Object> data = new ArrayList<Object>();
                    data = (List<Object>) cypherResult.get(column.getKey());
                    data.add(column.getValue());
                    cypherResult.put(column.getKey(), data);
                }
            }
            tx2.success();
            return cypherResult;
        }
    }
 */
