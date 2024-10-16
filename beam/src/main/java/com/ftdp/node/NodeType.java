package com.ftdp.node;

public enum NodeType {
    SOURCE("source",1),
    PROCESSOR("processor",2),
    SINK("sink",3);
    private String name;
    private int id;
    private NodeType(String name,int id) {
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
