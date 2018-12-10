package sc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapProcess {

    public final Map<String, Object> map;
    MapProcess.CleanObject cleanObject = new MapProcess.CleanObject();

    public MapProcess() {
        map = new HashMap();
    }

    public Map<String, Object> getMapAll() {
        return map;
    }

    public Map<String, Object> addToMap(String name, Map<String, Object> mapTmp) {
        map.put(name, mapTmp);
        return mapTmp;
    }

    public void removeFromMap(String name) {
        map.remove(name);
    }

    public Map<String, Object> getMapElementByName(String name) {
        Map<String, Object> mapTmp = (Map) map.get(name);
        return mapTmp;
    }

    public Map<String, Object> getMapElementByNameClean(String name) {
        Map<String, Object> mapTmp = (Map) map.get(name);
        return cleanObject.cleanMap(mapTmp);
    }

    public ArrayList<Map<String, Object>> getListFromMapAll() {
        List<String> mapKeys = new ArrayList(map.keySet());
        ArrayList<Map<String, Object>> listMap = new ArrayList();

        for (int i = 0; i < mapKeys.size(); i++) {
            Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

            listMap.add(mapTmp);
        }
        return listMap;
    }

    public ArrayList<Map<String, Object>> getListFromMapAllClean() {
        List<String> mapKeys = new ArrayList(map.keySet());
        ArrayList<Map<String, Object>> listMap = new ArrayList();

        for (int i = 0; i < mapKeys.size(); i++) {
            Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

            listMap.add(cleanObject.cleanMap(mapTmp));
        }
        return listMap;
    }

    public ArrayList<Map<String, Object>> getListFromMap() {
        List<String> mapKeys = new ArrayList(map.keySet());
        ArrayList<Map<String, Object>> listMap = new ArrayList();
        for (int i = 0; i < mapKeys.size(); i++) {
            Map<String, Object> mapTmp = (Map) map.get(mapKeys.get(i));

            listMap.add(mapTmp);
        }
        return listMap;
    }

    public class CleanObject {

        public CleanObject() {

        }

        public Map<String, Object> cleanMap(final Map<String, Object> mapInput) {
            final Map<String, Object> mapTmp = new HashMap<String, Object>();
            final List<String> mapKeys = new ArrayList<String>(mapInput.keySet());
            for (int i = 0; i < mapKeys.size(); ++i) {
                final Object mapObject = mapInput.get(mapKeys.get(i));
                if (mapObject instanceof String
                        || mapObject instanceof Integer
                        || mapObject instanceof Boolean
                        || mapObject instanceof Float
                        || mapObject instanceof Double
                        || mapObject instanceof Map) {
                    mapTmp.put(mapKeys.get(i), mapObject);
                }
            }
            return mapTmp;
        }
    }

}
