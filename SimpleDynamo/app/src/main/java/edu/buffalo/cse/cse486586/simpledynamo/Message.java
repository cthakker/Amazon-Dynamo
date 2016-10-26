package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by chintan on 4/25/16.
 */


public class Message implements Serializable{

    private String key;
    private String value;
    private String msgType;
    private String senderPort;
    private String remotePort;
    private HashMap<String, String> result;
    private int messagID;

    public void setValue(String value) {
        this.value = value;
    }

    public void setMessagID(int messagID) {
        this.messagID = messagID;
    }

    public int getMessagID() {
        return messagID;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setRemotePort(String remotePort) {
        this.remotePort = remotePort;
    }

    public void setSenderPort(String senderPort) {
        this.senderPort = senderPort;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getRemotePort() {
        return remotePort;
    }

    public String getSenderPort() {
        return senderPort;
    }

    public String getValue() {
        return value;
    }

    public String getKey() {
        return key;
    }

    public void setResult(HashMap<String, String> result) {
        this.result = result;
    }

    public HashMap<String, String> getResult() {
        return result;
    }
}