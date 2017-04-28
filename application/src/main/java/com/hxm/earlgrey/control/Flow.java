package com.hxm.earlgrey.control;

/**
 * Created by hxm on 17-3-16.
 */
public class Flow {
    private String srcAddress;
    private String dstAddress;
    private int srcPort;
    private int dstPort;
    private String time;
    private int duration;
    private String protocol;
    private int bytes;
    private int packages;
    private byte tos;

    public String getSrcAddress() {
        return srcAddress;
    }

    public void setSrcAddress(String srcAddress) {
        this.srcAddress = srcAddress;
    }

    public String getDstAddress() {
        return dstAddress;
    }

    public void setDstAddress(String dstAddress) {
        this.dstAddress = dstAddress;
    }

    public int getSrcPort() {
        return srcPort;
    }

    public void setSrcPort(int srcPort) {
        this.srcPort = srcPort;
    }

    public int getDstPort() {
        return dstPort;
    }

    public void setDstPort(int dstPort) {
        this.dstPort = dstPort;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public byte getTos() {
        return tos;
    }

    public void setTos(byte tos) {
        this.tos = tos;
    }

    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }

    public int getPackages() {
        return packages;
    }

    public void setPackages(int packages) {
        this.packages = packages;
    }
}
