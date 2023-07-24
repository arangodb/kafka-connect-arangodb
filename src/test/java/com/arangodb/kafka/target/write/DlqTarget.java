package com.arangodb.kafka.target.write;

import java.util.Map;

public class DlqTarget extends BaseWriteTarget {

    public DlqTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put("errors.deadletterqueue.topic.name", getName() + "-DLQ");
        cfg.put("errors.deadletterqueue.context.headers.enable", "true");
        cfg.put("errors.tolerance", "all");
        return cfg;
    }

}
