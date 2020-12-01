package com.kschool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Processor extends Thread {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    LinkedBlockingQueue<Map<String, Object>> outQueue;

    LocalMemCache localMemCache = new LocalMemCache();

    public enum Alert {
        USER_ACESS_WITHOUT_EXIT(0), USER_EXIT_WITHOUT_ACESS(1);

        public Integer get;

        Alert(Integer alert) {
            this.get = alert;
        }
    }

    public enum Control {
        OPEN(0), NOT_OPEN(1);

        public Integer get;

        Control(Integer action) {
            this.get = action;
        }
    }

    public enum EventType {
        CONTROL("control"), ALERT("alert"), METRIC("metric");

        public String get;

        EventType(String name) {
            this.get = name;
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

                //TODO: Ejercicio 6

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
