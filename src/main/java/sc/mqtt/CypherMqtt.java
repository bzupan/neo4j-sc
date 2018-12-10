package sc.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import sc.MapProcess;

public class CypherMqtt {

    private static final MapProcess mqttBrokersMap = new MapProcess();
    private static final String messagePublishDefaults = "{messageFormat: 'string'}";
    private static final String messageSubscribeDefaults = "{messageFormat: 'string'}";

    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // ----------------------------------------------------------------------------------
    // list
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.list() - list all CYPHER java VM calls")
    public List< Map<String, Object>> list() {
        log.debug("sc.mqtt.list: " + mqttBrokersMap.getListFromMapAllClean().toString());
        return mqttBrokersMap.getListFromMapAllClean();
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.add('mqttBrokerName', {brokerUrl:'tcp://iot.eclipse.org:1883' ,clientId:'123'  }) //- add CYPHER Java VM calls")
    public Map<String, Object> add(
            @Name("name") String name,
            @Name("mqtt") Map<String, Object> mqtt
    ) {
        String brokerUrl = mqtt.get("brokerUrl").toString();
        String clientId = name; //mqtt.get("clientId").toString();

        MemoryPersistence persistence = new MemoryPersistence();

        Map<String, Object> mqttBrokerTmp = new HashMap<String, Object>();

        try {
            MqttClientNeo mqttBrokerNeo4jClient = new MqttClientNeo(brokerUrl, clientId, persistence);
            log.debug("sc.mqtt -  connect ok: " + name + " " + brokerUrl + " " + clientId);
            mqttBrokerTmp.put("name", name);
            mqttBrokerTmp.put("mqtt", mqtt);
            mqttBrokerTmp.put("messageSendOk", 0);
            mqttBrokerTmp.put("messageSendError", 0);
            mqttBrokerTmp.put("messageSendErrorMessage", 0);
            mqttBrokerTmp.put("messageSubscribeOk", 0);
            mqttBrokerTmp.put("messageSubscribeError", 0);
            mqttBrokerTmp.put("messageSubscribeErrorMessage", 0);
            mqttBrokerTmp.put("mqttBrokerNeo4jClient", mqttBrokerNeo4jClient);
            mqttBrokerTmp.put("publishList", "");
            mqttBrokerTmp.put("subscribeList", "");
            mqttBrokersMap.addToMap(name, mqttBrokerTmp);
            return mqttBrokersMap.getMapElementByNameClean(name);
        } catch (Exception ex) {
            log.error("sc.mqtt -  connect error: " + name + " " + brokerUrl + " " + clientId + " " + ex.toString());
            return null;
        }

    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.delete('mqttBrokerName') - add CYPHER Java VM calls")
    public Map<String, Object> delete(
            @Name("name") String name
    ) {
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");

        if (!mqttBrokerNeo4jClient.equals(null)) {
            mqttBrokerNeo4jClient.unsubscribe();
            mqttBrokerNeo4jClient.disconnect();
            mqttBrokersMap.removeFromMap(name);
        }
        log.debug("sc.mqtt -  unsubscribe delete: " + name + " " + name);
        return null;
    }

    // ----------------------------------------------------------------------------------
    // run
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.publish('mqttBrokerName', '/mqtt/topic/path','message') - run CYPHER from Java VM)")
    public Stream<MapResult> publish(
            @Name("name") String name,
            @Name("topic") String toppic,
            @Name("message") Object message,
            @Name(value = "messagePublishOptions", defaultValue = messagePublishDefaults) Map<String, Object> messagePublishOptions
    ) {
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);

        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        String mqttMesageString = "";
        try {

            if (message instanceof String) {
                mqttMesageString = (String) message;
            } else {
                ObjectMapper mapper = new ObjectMapper();
                mqttMesageString = mapper.writeValueAsString(message).toString();
            }
            mqttBrokerNeo4jClient.publish(toppic, mqttMesageString);
            log.debug("sc.mqtt -  publish ok: " + name + " " + toppic + " " + message);
            mqttBroker.put("messageSendOk", 1 + (int) mqttBroker.get("messageSendOk"));

            //return "publish ok";
        } catch (Exception ex) {
            mqttBroker.put("messageSendError", 1 + (int) mqttBroker.get("messageSendError"));
            mqttBroker.put("messageSendErrorMessage", "sc.mqtt -  publish error: " + name + " " + toppic + " " + mqttMesageString + " " + ex.toString());
            log.error("sc.mqtt -  publish error: " + name + " " + toppic + " " + mqttMesageString + " " + ex.toString());
        }
        return mqttBrokersMap.getListFromMapAllClean().stream().map(MapResult::new);
    }

    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.subscribe('mqttBrokerName', '/mqtt/topic/path','cypherQuery', )- run CYPHER from Java VM)")
    public Stream<MapResult> subscribe(
            @Name("name") String name,
            @Name("topic") String toppic,
            @Name("query") String query,
            @Name(value = "subscribeOptions", defaultValue = messageSubscribeDefaults) Map<String, Object> subscribeOptions
    ) {
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);

        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        ProcessMqttMessage task = new ProcessMqttMessage();
        try {
            mqttBroker.put("publishList", mqttBroker.get("publishList").toString() + " " + name + " " + toppic + " " + query  );
            mqttBrokerNeo4jClient.listen(toppic, query, task);

            mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));
           
        } catch (Exception ex) {
            log.error("");
            mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
            mqttBroker.put("messageSubscribeErrorMessage", "sc.mqtt -  subscribe error: " + name + " " + toppic + " " + query + " " + ex.toString());
            //   mqttBroker.put("messageSubscribeErrorMessage", 0);  
        }

        return null;
    }

    // ----------------------------------------------------------------------------------
    // util
    // ----------------------------------------------------------------------------------
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

    public class ProcessMqttMessage {

        public ProcessMqttMessage() {
        }

        public void run(Object message) {
            JSONUtils checkJson = new JSONUtils();
            System.out.println("Message: received " + message.toString());

            //System.out.print(checkJson.jsonStringToMap(message.toString()));
        }

        public void run(String cypherQuery, String message) {

            JSONUtils checkJson = new JSONUtils();
            System.out.println("Message: received " + cypherQuery + " " +message.toString());
            db.execute(cypherQuery, (Map<String, Object>) checkJson.jsonStringToMap(message)); //System.out.print(checkJson.jsonStringToMap(message.toString()));
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
    public class MqttClientNeo {

        int qos = 2;
        public MqttClient sampleClient;

        // four constructors
        public MqttClientNeo(String broker, String clientId, MemoryPersistence persistence) throws MqttException {
            this.sampleClient = new MqttClient(broker, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            this.sampleClient.connect(connOpts);

            // --- send connect message
            String messageTmp = clientId + " connected to " + broker;
            MqttMessage message = new MqttMessage(messageTmp.getBytes());
            message.setQos(qos);
            this.sampleClient.publish("system", message);
            //System.out.println("sc.mqtt -  connect ok: " + clientId + " " + broker);

        }

        private String publish(String topic, String content) throws MqttException {
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();

            this.sampleClient.publish(topic, message);
            //System.out.println("sc.mqtt -  publish ok: " + clientId + " " + broker + " " + content);
            return "publish ok";

        }

        private void unsubscribe() {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            try {
                this.sampleClient.unsubscribe("#");
                //System.out.println("sc.mqtt -  unsubscribe ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                System.out.println("sc.mqtt -  unsubscribe error: " + clientId + " " + broker + " " + ex.toString());
            }
        }

        private void disconnect() {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            try {
                this.sampleClient.disconnect();
                // System.out.println("sc.mqtt -  disconnect ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                System.out.println("sc.mqtt -  disconnect error: " + clientId + " " + broker + " " + ex.toString());
            }

        }

        public void listen(String topic, String query, ProcessMqttMessage task) throws MqttException {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            this.sampleClient.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                }

                public void messageArrived(String topic, MqttMessage message) {
                    task.run(query, message.toString());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            this.sampleClient.subscribe(topic);

        }

    }

}
