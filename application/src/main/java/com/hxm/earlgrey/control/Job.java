package com.hxm.earlgrey.control;


import java.util.Map;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Created by hxm on 17-2-13.
 */
@Document(collection = "Jobs")
public class Job {
    public enum TYPE {Test, FlowQuery}

    public enum STATUS {Created, Started, End, Failed, Error, Canceled}

    public static final String ID_DATE_FORMAT = "yyyyMMddHHmmss";
    public static final String DATE_FORMAT = "yyyy-MM-dd-HH:mm:ss";

    public static String JAVA_COMMAND = "java";

    public static final String PARAM_NAME = "name";
    public static final String PARAM_TYPE = "type";
    public static final String PARAM_RUNNING_TIME = "running_time";
    public static final String PARAM_STARTTIME = "start";
    public static final String PARAM_ENDTIME = "end";
    public static final String PARAM_SRCADDRESS = "src_addr";
    public static final String PARAM_DSTADDRESS = "dst_addr";
    public static final String PARAM_SRCPORT = "src_port";
    public static final String PARAM_DSTPORT = "dst_port";
    public static final String PARAM_PROTOCOL = "prot";
    public static final String PARAM_TOS = "tos";


    private String id;
    private TYPE type;
    private String name;
    private String startedTime;
    private String endedTime;
    private Map<String, String> params;
    private STATUS status = STATUS.Created;
    private int progress;
    private String statusDetail;


    public Job() {
    }

    public Job(String id, TYPE type, String name, Map<String, String> params) {
        this.id = id;
        this.type = type;
        this.name = name;
        this.params = params;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public TYPE getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getStartedTime() {
        return startedTime;
    }

    public void setStartTime(String startedTime) {
        this.startedTime = startedTime;
    }

    public String getEndedTime() {
        return endedTime;
    }

    public void setEndedTime(String endedTime) {
        this.endedTime = endedTime;
    }

    public STATUS getStatus() {
        return status;
    }

    public void setStatus(STATUS status) {
        this.status = status;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getStatusDetail() {
        return statusDetail;
    }

    public void setStatusDetail(String statusDetail) {
        this.statusDetail = statusDetail;
    }
}
