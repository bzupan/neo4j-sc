package sc.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Stream;

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
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import sc.MapResult;
import sc.MapProcess;

public class Neo4jMqtt {

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
    @Description("RETURN sc.mqtt.listBrokers() ")
    public List< Map<String, Object>> listBrokers() {
        log.debug("sc.mqtt.listBrokers: " + mqttBrokersMap.getListFromMapAllClean().toString());
        return mqttBrokersMap.getListFromMapAllClean();
    }

    // ----------------------------------------------------------------------------------
    // add
    // ----------------------------------------------------------------------------------
    // RETURN sc.mqtt.addBroker('graphTravelerMaster', {brokerUrl:'tcp://10.20.20.13:1883' ,clientId:'neo4jDb'  })
    @UserFunction
    @Description("RETURN sc.mqtt.addBroker('mqttBrokerName', {brokerUrl:'tcp://iot.eclipse.org:1883' ,clientId:'123'  })   // add MqTT broker client")
    public Map<String, Object> addBroker(
            @Name("name") String name,
            @Name("mqtt") Map<String, Object> mqtt
    ) {
        log.debug("sc.mqtt.addBroker: " + name + " " + mqtt.toString());
        String brokerUrl = mqtt.get("brokerUrl").toString();
        String clientId = name; //mqtt.get("clientId").toString();

        if (mqtt.get("clientId").equals(null)) {
            clientId = name + "Client" + new Random().nextInt();
        } else {
            clientId = mqtt.get("clientId").toString();
        }

        MemoryPersistence persistence = new MemoryPersistence();
        Map<String, Object> mqttBrokerTmp = new HashMap<String, Object>();

        try {
            MqttClientNeo mqttBrokerNeo4jClient = new MqttClientNeo(brokerUrl, clientId, persistence);
            log.debug("sc.mqtt -  connect ok: " + name + " " + brokerUrl + " " + clientId);
            mqttBrokerTmp.put("name", name);
            mqttBrokerTmp.put("clientId", clientId);
            mqttBrokerTmp.put("mqtt", mqtt);
            mqttBrokerTmp.put("messageSendOk", 0);
            mqttBrokerTmp.put("messageSendError", 0);
            mqttBrokerTmp.put("messageSubscribeOk", 0);
            mqttBrokerTmp.put("messageSubscribeError", 0);
            mqttBrokerTmp.put("messageSubscribeReceived", 0);
            mqttBrokerTmp.put("mqttBrokerNeo4jClient", mqttBrokerNeo4jClient);
            Map<String, Object> subscribeList = new HashMap<String, Object>();
            mqttBrokerTmp.put("subscribeList", subscribeList);
            mqttBrokersMap.addToMap(name, mqttBrokerTmp);
            log.info("sc.mqtt - addBroker ok: " + name + " " + brokerUrl + " " + clientId);
            return mqttBrokersMap.getMapElementByNameClean(name);
        } catch (Exception ex) {
            log.error("sc.mqtt - addBroker error: " + name + " " + brokerUrl + " " + clientId + " " + ex.toString());
            return null;
        }
    }

    // ----------------------------------------------------------------------------------
    // delete
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("RETURN sc.mqtt.deleteBroker('mqttBrokerName') // delete MqTT broker client")
    public Map<String, Object> deleteBroker(
            @Name("name") String name
    ) {
        // --- delete broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");

        if (!mqttBrokerNeo4jClient.equals(null)) {
            mqttBrokerNeo4jClient.unsubscribeAll();
            mqttBrokerNeo4jClient.disconnect();
            mqttBrokersMap.removeFromMap(name);
        }
        log.debug("sc.mqtt -  deleteBroker: " + name + " " + name);
        return null;
    }

    // ----------------------------------------------------------------------------------
    // publish
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.publishValue('mqttBrokerName', '/mqtt/topic/path', 'value'}))")
    public Stream<MapResult> publishValue(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("value") Object value,
            @Name(value = "messagePublishOptions", defaultValue = messagePublishDefaults) Map<String, Object> messagePublishOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- send message
        String mqttMesageString = (String) value;
        try {
            mqttBrokerNeo4jClient.publish(topic, mqttMesageString);
            log.debug("sc.mqtt - publishValue ok:\n" + name + "\n" + topic + "\n" + mqttMesageString);
            mqttBroker.put("messageSendOk", 1 + (int) mqttBroker.get("messageSendOk"));
        } catch (Exception ex) {
            mqttBroker.put("messageSendError", 1 + (int) mqttBroker.get("messageSendError"));
            mqttBroker.put("messageSendErrorMessage", "sc.mqtt - publish error: " + name + " " + topic + " " + mqttMesageString + " " + ex.toString());
            log.error("sc.mqtt - publishValue error:\n" + name + "\n" + topic + "\n" + mqttMesageString + "\n" + ex.toString());
        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.publishJson('mqttBrokerName', '/mqtt/topic/path', {message:123})")
    public Stream<MapResult> publishJson(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("message") Object message,
            @Name(value = "messagePublishOptions", defaultValue = messagePublishDefaults) Map<String, Object> messagePublishOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- send message
        String mqttMesageString = "";
        try {
            if (message instanceof String) {
                mqttMesageString = (String) message;
            } else {
                ObjectMapper mapper = new ObjectMapper();
                mqttMesageString = mapper.writeValueAsString(message).toString();
            }
            mqttBrokerNeo4jClient.publish(topic, mqttMesageString);
            log.debug("sc.mqtt - publishJson ok:\n" + name + "\n" + topic + "\n" + mqttMesageString);
            mqttBroker.put("messageSendOk", 1 + (int) mqttBroker.get("messageSendOk"));
        } catch (Exception ex) {
            mqttBroker.put("messageSendError", 1 + (int) mqttBroker.get("messageSendError"));
            mqttBroker.put("messageSendErrorMessage", "sc.mqtt - publishJson error: " + name + " " + topic + " " + mqttMesageString + " " + ex.toString());
            log.error("sc.mqtt - publishJson error:\n" + name + "\n" + topic + "\n" + mqttMesageString + "\n" + ex.toString());
        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.publishJsonRpc2('graphTravelerMaster', 'pingIpAddress', {ipAddress:'10.20.20.13'}) ")
    public Stream<MapResult> publishJsonRpc2(
            @Name("name") String name,
            @Name("method") String method,
            @Name("message") Object message,
            @Name(value = "messagePublishOptions", defaultValue = messagePublishDefaults) Map<String, Object> messagePublishOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- set message
        String mqttMesageString = "";
        Map<String, Object> jsonRpc2Request = new HashMap();
        Map<String, Object> jsonRpc2Params = new HashMap();
        jsonRpc2Request.put("jsonrpc", "2.0");
        jsonRpc2Request.put("method", method);
        jsonRpc2Request.put("id", new Random().nextInt());
        if (message instanceof Map) {
            jsonRpc2Params = (Map<String, Object>) message;
            jsonRpc2Request.put("params", jsonRpc2Params);
        } else if (message instanceof Node) {
            jsonRpc2Params = (Map<String, Object>) ((Node) message).getAllProperties();
            jsonRpc2Request.put("id", ((Node) message).getId());
            jsonRpc2Request.put("params", jsonRpc2Params);
        } else if (message instanceof Relationship) {
            jsonRpc2Params = (Map<String, Object>) ((Relationship) message).getAllProperties();
            jsonRpc2Request.put("id", ((Relationship) message).getId());
            jsonRpc2Request.put("params", jsonRpc2Params);
        } else {
            log.error("sc.mqtt - publishJsonRpc2 error - wrong message:\n" + method + "\n" + message);
            return null;
        }
        // --- set topic
        String mqttServerId = (String) mqttBroker.get("name");
        String mqttClientId = (String) mqttBroker.get("clientId");
        String mqttTopic = "/mqttRpc/request/" + mqttServerId + "/" + method + "/" + mqttClientId;
        // --- send message
        try {
            ObjectMapper mapper = new ObjectMapper();
            log.info("sc.mqtt -  publishJsonRpc2: " + jsonRpc2Request);
            mqttMesageString = mapper.writeValueAsString(jsonRpc2Request).toString();
            mqttBrokerNeo4jClient.publish(mqttTopic, mqttMesageString);
            log.info("sc.mqtt - publishJsonRpc2 ok: " + name + "\n" + mqttTopic + "\n" + mqttMesageString);
            mqttBroker.put("messageSendOk", 1 + (int) mqttBroker.get("messageSendOk"));
        } catch (Exception ex) {
            mqttBroker.put("messageSendError", 1 + (int) mqttBroker.get("messageSendError"));
            mqttBroker.put("messageSendErrorMessage", "sc.mqtt - publishJsonRpc2 error: " + name + " " + mqttTopic + " " + mqttMesageString + " " + ex.toString());
            log.error("sc.mqtt - publishJsonRpc2 error: " + name + "\n" + mqttTopic + "\n" + mqttMesageString + "\n" + ex.toString());
        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    // subscribe
    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.subscribeValue('mqttBrokerName', '/mqtt/topic/path','MERGE (n:mqttTest) ON CREATE SET n.count=1, n.message=$message ON MATCH SET n.count = n.count +1, n.message=$message ') ")
    public Stream<MapResult> subscribeValue(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("query") String query,
            @Name(value = "subscribeOptions", defaultValue = messageSubscribeDefaults) Map<String, Object> subscribeOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- set processor
        String messageType = (String) subscribeOptions.get("messageType");
        ProcessMqttMessage task = new ProcessMqttMessage("value", query);
        // --- subscribe
        try {
            // --- add to subscription list
            Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
            subscribeList.put(topic, query);
            mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));
            mqttBrokerNeo4jClient.subscribe(topic, query, task);
            log.debug("sc.mqtt - subscribeValue ok: \n" + name + "\n" + topic);
        } catch (Exception ex) {
            mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
            //mqttBroker.put("messageSubscribeErrorMessage", "sc.mqtt -  subscribe error: " + name + " " + topic + " " + query + " " + ex.toString());
            log.error("sc.mqtt -  subscribeValue error: \n" + name + "\n" + topic + "\n" + "\n" + ex.toString());

        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.subscribeJson('mqttBrokerName', '/mqtt/topic/path','MERGE (n:mqttTest) ON CREATE SET n.count=1, n.message=$message ON MATCH SET n.count = n.count +1, n.message=$message ') ")
    public Stream<MapResult> subscribeJson(
            @Name("name") String name,
            @Name("topic") String topic,
            @Name("query") String query,
            @Name(value = "subscribeOptions", defaultValue = messageSubscribeDefaults) Map<String, Object> subscribeOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- set processor
        String messageType = (String) subscribeOptions.get("messageType");
        ProcessMqttMessage task = new ProcessMqttMessage("json", query);
        // --- subscribe
        try {
            // --- add to subscription list
            Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
            subscribeList.put(topic, query);
            mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));

            mqttBrokerNeo4jClient.subscribe(topic, query, task);
            log.debug("sc.mqtt -  subscribeJson ok: \n" + name + "\n" + topic);
        } catch (Exception ex) {
            mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
            //mqttBroker.put("messageSubscribeErrorMessage", "sc.mqtt -  subscribe error: " + name + " " + topic + " " + query + " " + ex.toString());
            log.error("sc.mqtt -  subscribeJson error: \n" + name + "\n" + topic + "\n" + "\n" + ex.toString());

        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    @Procedure(mode = Mode.WRITE)
    @Description("CALL sc.mqtt.subscribeJsonRpc2('graphTravelerMaster', 'pingIpAddress','MERGE (n:mqttTest) ON CREATE SET n.count=1, n.message=$message ON MATCH SET n.count = n.count +1, n.message=$message ')")
    public Stream<MapResult> subscribeJsonRpc2(
            @Name("name") String name,
            @Name("method") String method,
            @Name("query") String query,
            @Name(value = "subscribeOptions", defaultValue = messageSubscribeDefaults) Map<String, Object> subscribeOptions
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);
        // --- set topic
        String mqttServerId = (String) mqttBroker.get("name");
        String mqttClientId = (String) mqttBroker.get("clientId");
        String mqttTopic = "/mqttRpc/response/" + mqttClientId + "/" + method + "/" + mqttServerId;
        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        // --- set processor
        ProcessMqttMessage taskJsonRpc2 = new ProcessMqttMessage("jsonRpc2", query);
        // --- subscribe
        try {
            // --- add to subscription list
            Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
            subscribeList.put(mqttTopic, query);
            mqttBroker.put("messageSubscribeOk", 1 + (int) mqttBroker.get("messageSubscribeOk"));

            mqttBrokerNeo4jClient.subscribe(mqttTopic, query, taskJsonRpc2);
            log.debug("sc.mqtt -  subscribeJsonRpc2 ok:\n" + name + "\n" + mqttTopic);

        } catch (Exception ex) {
            mqttBroker.put("messageSubscribeError", 1 + (int) mqttBroker.get("messageSubscribeError"));
            //mqttBroker.put("messageSubscribeErrorMessage", "sc.mqtt -  subscribe error: " + name + " " + topic + " " + query + " " + ex.toString());
            log.error("sc.mqtt -  subscribeJsonRpc2 error:\n" + name + "\n" + mqttTopic + "\n" + "\n" + ex.toString());

        }
        return Stream.of(mqttBrokersMap.getMapElementByNameClean(name)).map(MapResult::new);
    }

    // ----------------------------------------------------------------------------------
    // unsubscribe
    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("CALL sc.mqtt.unSubscribeTopic('mqttBrokerName', '/mqtt/topic/path' )")
    public Object unSubscribeTopic(
            @Name("name") String name,
            @Name("topic") String topic
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);

        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");
        mqttBrokerNeo4jClient.unsubscribe(topic);

        Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
        subscribeList.remove(topic);
        log.debug("sc.mqtt - unSubscribeTopic: " + name + " " + topic);

        return null;
    }

    // ----------------------------------------------------------------------------------
    @UserFunction
    @Description("CALL sc.mqtt.unSubscribeJsonRpc2('mqttBrokerName', 'pingIpAddress' )")
    public Object unSubscribeJsonRpc2(
            @Name("name") String name,
            @Name("method") String method
    ) {
        // --- get broker
        Map<String, Object> mqttBroker = mqttBrokersMap.getMapElementByName(name);

        MqttClientNeo mqttBrokerNeo4jClient = (MqttClientNeo) mqttBroker.get("mqttBrokerNeo4jClient");

        // --- set topic
        String mqttServerId = (String) mqttBroker.get("name");
        String mqttClientId = (String) mqttBroker.get("clientId");
        String mqttTopic = "/mqttRpc/response/" + mqttClientId + "/" + method + "/" + mqttServerId;

        mqttBrokerNeo4jClient.unsubscribe(mqttTopic);

        Map<String, Object> subscribeList = (Map<String, Object>) mqttBroker.get("subscribeList");
        subscribeList.remove(mqttTopic);
        log.debug("sc.mqtt - unSubscribeJsonRpc2: " + name + " " + mqttTopic);

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

    // ----------------------------------------------------------------------------------
    // ProcessMqttMessage
    // ----------------------------------------------------------------------------------
    public class ProcessMqttMessage {

        String processType = "";
        String cypherQuery = "";

        public ProcessMqttMessage(String messageType, String cypherQueryInput) {

            this.processType = messageType;
            this.cypherQuery = cypherQueryInput;
            log.info("sc.mqtt - ProcessMqttMessage registration: " + this.processType + this.cypherQuery);
        }

        public void run(String message) {
            log.info("sc.mqtt - ProcessMqttMessage run: " + this.cypherQuery + " " + message + " " + this.processType);

            Map<String, Object> cypherParams = new HashMap();
            if (this.processType == "json") {
                JSONUtils checkJson = new JSONUtils();
                cypherParams = (Map<String, Object>) checkJson.jsonStringToMap(message);
            } else if (this.processType == "jsonRpc2") {
                JSONUtils checkJson = new JSONUtils();
                Map<String, Object> jsonRpc2responseResult = (Map<String, Object>) checkJson.jsonStringToMap(message);

                if (!(jsonRpc2responseResult.get("result") == null)) {
                    cypherParams = (Map<String, Object>) jsonRpc2responseResult.get("result");
                    cypherParams.put("id", cypherParams.get("id"));
                } else {
                    log.error("sc.mqtt - message received error:\n" + cypherParams.toString());
                }

            } else if (this.processType == "value") {
                cypherParams.put("value", message);
            } else {
                cypherParams = new HashMap();
            }

            log.info("sc.mqtt - message received: \n" + this.cypherQuery + "\n" + message + "\n" + this.processType + "\n" + cypherParams.toString());
            try (Transaction tx = db.beginTx()) {
                Result dbResult = db.execute(this.cypherQuery, cypherParams);
                log.debug("sc.mqtt - cypherQuery results:\n" + "\n" + dbResult.resultAsString());
                tx.success();
            } catch (Exception ex) {
                log.error("sc.mqtt - cypherQuery error:\n" + ex.toString());
            }
        }

    }

    // ----------------------------------------------------------------------------------
    // MqttClientNeo
    // ----------------------------------------------------------------------------------
    public class MqttClientNeo {

        int qos = 2;
        public MqttClient sampleClient;
        Map<String, Object> mapMqttTopicTask = new HashMap<>();

        // four constructors
        public MqttClientNeo(String broker, String clientId, MemoryPersistence persistence) throws MqttException {
            sampleClient = new MqttClient(broker, clientId, persistence);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setAutomaticReconnect(true);
            connOpts.setCleanSession(false);

            sampleClient.connect(connOpts);

            sampleClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    log.debug("connectionLost");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    log.debug("messageArrived " + topic + " " + message.toString());
                    ProcessMqttMessage task = (ProcessMqttMessage) mapMqttTopicTask.get(topic);
                    log.debug("aaa" + task.toString() + task.cypherQuery + task.processType);
                    task.run(message.toString());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    log.debug("deliveryComplete");
                }
            });

            // --- send connect message
            String messageTmp = clientId + " connected to " + broker;
            log.info("sc.mqtt -  connect ok: " + clientId + " " + broker);
        }

        private void publish(String topic, String content) throws MqttException {
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();

            this.sampleClient.publish(topic, message);

            log.debug("publish" + mapMqttTopicTask.toString());

        }

        private void unsubscribeAll() {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            mapMqttTopicTask = null;
            try {
                this.sampleClient.unsubscribe("#");
                log.info("sc.mqtt -  unsubscribeAll ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("sc.mqtt -  unsubscribeAll error: " + clientId + " " + broker + " " + ex.toString());
            }
        }

        private void unsubscribe(String topic) {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            mapMqttTopicTask.remove(topic);
            try {
                this.sampleClient.unsubscribe(topic);
                log.info("sc.mqtt -  unsubscribe ok: " + topic + " " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("sc.mqtt -  unsubscribe error: " + topic + " " + clientId + " " + broker + " " + ex.toString());
            }
        }

        private void disconnect() {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            mapMqttTopicTask = null;
            try {
                this.sampleClient.disconnect();
                log.info("sc.mqtt -  disconnect ok: " + clientId + " " + broker);
            } catch (MqttException ex) {
                log.error("sc.mqtt -  disconnect error: " + clientId + " " + broker + " " + ex.toString());
            }

        }

        public void subscribe(String topic, String query, ProcessMqttMessage task) throws MqttException {
            String clientId = this.sampleClient.getClientId();
            String broker = this.sampleClient.getServerURI();
            log.info("sc.mqtt - subscribe: " + topic + " " + clientId + " " + broker + " " + query);

            mapMqttTopicTask.put(topic, task);
            this.sampleClient.subscribe(topic);
        }
    }
}
