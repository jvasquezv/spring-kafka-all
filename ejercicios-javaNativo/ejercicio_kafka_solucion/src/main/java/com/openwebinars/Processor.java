package com.openwebinars;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Processor extends Thread {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    LinkedBlockingQueue<Map<String, Object>> outQueue;

    LocalMemCache localMemCache = new LocalMemCache();

    public enum Alert {
        USER_ACESS_WITHOUT_EXIT(0), USER_EXIT_WITHOUT_ACESS(1);

        public Integer alert;

        Alert(Integer alert) {
            this.alert = alert;
        }
    }

    public enum Control {
        OPEN(0), NOT_OPEN(1);

        public Integer action;

        Control(Integer action) {
            this.action = action;
        }
    }

    public enum EventType {
        CONTROL("control"), ALERT("alert"), METRIC("metric");

        public String type;

        EventType(String name) {
            this.type = name;
        }
    }

    public Processor(LinkedBlockingQueue<Map<String, Object>> inQueue,
                     LinkedBlockingQueue<Map<String, Object>> outQueue) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                Map<String, Object> inEvent = inQueue.take();

                Integer timestamp = (Integer) inEvent.get("timestamp");
                Integer action = (Integer) inEvent.get("action");
                String userId = (String) inEvent.get("user_id");

                Integer oldTimestamp = localMemCache.getUserTime(userId);
                Map<String, Object> controlEvent = new HashMap<>();
                controlEvent.put("type", EventType.CONTROL.type);
                
                if (action.equals(0)) {
                    if (oldTimestamp != null) {
                        Map<String, Object> alertEvent = new HashMap<>();
                        alertEvent.put("type", "alert");
                        alertEvent.put("alert", Alert.USER_ACESS_WITHOUT_EXIT.alert);
                        alertEvent.put("user_id", userId);
                        alertEvent.put("full_name", inEvent.get("full_name"));
                        alertEvent.put("timestamp", timestamp);
                        outQueue.put(alertEvent);

                        controlEvent.put("action", Control.NOT_OPEN.action);
                    } else {
                        localMemCache.saveUserTime(userId, timestamp);
                        controlEvent.put("action", Control.OPEN.action);
                    }

                    controlEvent.put("user_id", userId);
                    controlEvent.put("timestamp", timestamp);

                } else if (action.equals(1)) {
                    if (oldTimestamp == null) {
                        Map<String, Object> alertEvent = new HashMap<>();
                        alertEvent.put("type", EventType.ALERT.type);
                        alertEvent.put("alert", Alert.USER_EXIT_WITHOUT_ACESS.alert);
                        alertEvent.put("user_id", userId);
                        alertEvent.put("full_name", inEvent.get("full_name"));
                        alertEvent.put("timestamp", timestamp);
                        outQueue.put(alertEvent);

                        controlEvent.put("action", Control.NOT_OPEN.action);
                    } else {
                        localMemCache.removeUser(userId);
                        controlEvent.put("action", Control.OPEN.action);
                        Map<String, Object> metricEvent = new HashMap<>();
                        metricEvent.put("type", EventType.METRIC.type);
                        Integer duration = timestamp - oldTimestamp;
                        metricEvent.put("duration", duration);
                        metricEvent.put("user_id", userId);
                        metricEvent.put("full_name", inEvent.get("full_name"));
                        metricEvent.put("timestamp", timestamp);

                        outQueue.put(metricEvent);
                    }

                    controlEvent.put("user_id", userId);
                    controlEvent.put("timestamp", timestamp);

                    outQueue.put(controlEvent);
                }

            } catch (InterruptedException e) {
                System.out.println("Apagando el procesador ... ");
            }
        }
    }

    public void shutdown() {
        interrupt();
    }

    private class LocalMemCache {
        private Map<String, Integer> cache = new HashMap<>();

        public void saveUserTime(String userId, Integer timestamp) {
            cache.put(userId, timestamp);
        }

        public Boolean userExist(String userId) {
            return cache.containsKey(userId);
        }

        public Integer getUserTime(String userId) {
            return cache.get(userId);
        }

        public void removeUser(String userId) {
            cache.remove(userId);
        }
    }
}
