package com.hxm.earlgrey.control;

import static com.hxm.earlgrey.control.Job.ID_DATE_FORMAT;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by hxm on 17-2-13.
 */
@Service
public class JobService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private DateFormat df = new SimpleDateFormat(ID_DATE_FORMAT);
    public static final String COLLECTION_NAME_FLOWQUERY = "FlowQuery-";
    private long t = 0;
    private int count = 0;

    @Value("${spark.home}")
    private String sparkHome;


    @Autowired
    private JobsRepository repository;
    @Autowired
    private MongoTemplate mongoTemplate;

    private Map<String, Process> jobs = new HashMap<>();
    private Object lock = new Object();

    String create(String name, final Job.TYPE type, final Map<String, String> params) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("job name needed");

        switch (type) {
            case FlowQuery:
                String startTime = params.get(Job.PARAM_STARTTIME);
                if (startTime == null || startTime.isEmpty())
                    throw new IllegalArgumentException("Need start time");
                try {
                    df.parse(startTime);
                } catch (ParseException pe) {
                    throw new IllegalArgumentException("Start time format error");
                }

                String endTime = params.get(Job.PARAM_ENDTIME);
                if (endTime == null || endTime.isEmpty())
                    throw new IllegalArgumentException("Need end time");
                try {
                    df.parse(endTime);
                } catch (ParseException pe) {
                    throw new IllegalArgumentException("End time format error");
                }

                String srcAddress = params.get(Job.PARAM_SRCADDRESS);
                String dstAddress = params.get(Job.PARAM_DSTADDRESS);
                if (srcAddress == null && dstAddress == null)
                    throw new IllegalArgumentException("Need source or destination address");
                if (srcAddress != null) {
                    if (srcAddress.isEmpty())
                        throw new IllegalArgumentException("Source ip cannot be empty");
                    try {
                        InetAddress.getByName(srcAddress);
                    } catch (UnknownHostException uhe) {
                        throw new IllegalArgumentException("Illegal source ip");
                    }
                }
                if (dstAddress != null) {
                    if (dstAddress.isEmpty())
                        throw new IllegalArgumentException("Destination ip cannot be empty");
                    try {
                        InetAddress.getByName(dstAddress);
                    } catch (UnknownHostException uhe) {
                        throw new IllegalArgumentException("Illegal destination ip");
                    }
                }
                break;
            case Test:
                break;
            default:
                throw new IllegalArgumentException("Job type cannot be recognized");
        }

        //TODO: test job id
        Date d = new Date();
        String id = df.format(d);
        synchronized (lock) {
            if (t == d.getTime()) {
                id += "-" + ++count;
            } else {
                count = 0;
            }
            t = d.getTime();
        }
        Job j = new Job(id, type, name, params);
        repository.save(j);
        return id;
    }

    Job getJob(String jobId) throws JobException {
        if (jobId == null || jobId.isEmpty())
            throw new IllegalArgumentException("jobId cannot be null or empty");
        Job job = repository.findOne(jobId);
        if (job == null) throw new JobException("Job " + jobId + " not found");
        return job;
    }


    /**
     * log file name: logs/job-{type}-{id}.log
     * submit spark job deploy client
     *
     * @param jobId
     * @throws JobException
     */
    void start(final String jobId) throws JobException {
        final Job job = getJob(jobId);
        if (job.getStatus() != Job.STATUS.Created)
            throw new JobException("Job " + jobId + "cannot be started, status:" + job.getStatus());

        final CountDownLatch startedSignal = new CountDownLatch(1);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    List<String> command;
                    switch (job.getType()) {
                        case Test:
                            command = makeTestCommand(job);
                            break;
                        case FlowQuery:
                            command = makeFlowQueryCommand();
                            break;
                        default:
                            throw new IllegalArgumentException("Job type known " + job.getType());
                    }
                    ProcessBuilder pb = new ProcessBuilder(command);
                    pb.directory(new File(sparkHome));
                    //FIXME: directory should log put
                    File log = new File(sparkHome + "/logs/job-" + job.getType() + "-" + job.getId() + ".log");
                    log.createNewFile();
                    pb.redirectErrorStream(true);
                    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
                    //job status updated in job
                    Process p = pb.start();
                    jobs.put(jobId, p);
                    startedSignal.countDown();

                    p.waitFor();
                } catch (Exception e) {
                    logger.error("Start process error:", e);
                    job.setStatus(Job.STATUS.Failed);
                    job.setStatusDetail(e.getMessage());
                    repository.save(job);
                } finally {
                    jobs.remove(jobId);
                    startedSignal.countDown();
                }
            }
        });
        //t.setDaemon(true);
        t.start();
        try {
            startedSignal.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    void cancel(String jobId) throws JobException {
        Job job = getJob(jobId);
        Process jobProcess = jobs.get(jobId);
        //TODO: job in db, not in memory
        if (jobProcess == null) throw new JobException("Job not found in memory" + jobId);
        //TODO: exception handling
        jobProcess.destroy();
        job.setStatus(Job.STATUS.Canceled);
        repository.save(job);
        jobs.remove(jobId);
    }

    void delete(String jobId) throws JobException {
        Job job = getJob(jobId);
        if (job.getStatus() == Job.STATUS.Started)
            throw new IllegalStateException("Job " + jobId + " is in progress, cannot be deleted");
        switch (job.getType()) {
            case FlowQuery:
                String C_NAME = COLLECTION_NAME_FLOWQUERY + jobId;
                mongoTemplate.dropCollection(C_NAME);
                break;
            default:
                throw new IllegalArgumentException("Job type cannot be recognized");
        }
        repository.delete(jobId);
    }

    void logException(String jobId, Exception e) {
        logger.error("Job " + jobId + " exception", e);
        try {
            Job job = repository.findOne(jobId);
            //REFACTOR: show more message
            job.setStatusDetail(e.getMessage());
            job.setStatus(Job.STATUS.Error);
            repository.save(job);
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
    }

    @Value("${projectHome}")
    String projectHome;
    private List<String> makeTestCommand(Job job) {
        String testJarFile = projectHome + "/testjob/target/testjob-0.1.jar";

        List<String> command = new LinkedList<>();
        command.add(Job.JAVA_COMMAND);
        command.add("-jar");
        command.add(testJarFile);
        command.add(job.getId());
        String runningTime = job.getParams().get(Job.PARAM_RUNNING_TIME);
        if (runningTime == null)
            throw new IllegalArgumentException("Job test need parameter running time");
        command.add(runningTime);
        return command;
    }

    private List<String> makeFlowQueryCommand() {
        List<String> command = new LinkedList<>();
        return command;
    }
}
