package com.ftdp.node;

public enum NodeType {
    source("source", 1),
    processor("processor", 2),
    sink("sink", 3);
    private String name;
    private int id;

    private NodeType(String name, int id) {
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }
}
