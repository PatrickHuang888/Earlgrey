package com.hxm.earlgrey.control;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


/**
 * Created by hxm on 17-3-23.
 */
@RestController
@RequestMapping("/Jobs")
public class JobController {
    public static final String ACTION_START = "start";
    public static final String ACTION_CANCEL = "cancel";
    public static final String ACTION_DELETE = "delete";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private JobService jobService;


    /**
     * parameter time format 'yyyy-MM-dd-HH:mm:ss'
     * parameter name and type mandatory
     *
     * @return {jobId: xxx}
     */
    @RequestMapping(method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public @ResponseBody
    ResponseEntity<?> createJob(@RequestBody Map<String, String> params) {
        ResponseEntity<?> responseEntity;

        try {
            String name = params.get(Job.PARAM_NAME);
            if (name == null || name.isEmpty())
                throw new IllegalArgumentException("Need a job name");
            params.remove(Job.PARAM_NAME);
            String type = params.get(Job.PARAM_TYPE);
            if (type == null || type.isEmpty())
                throw new IllegalArgumentException("Job need type");
            params.remove(Job.PARAM_TYPE);

            String jobId = jobService.create(name, Job.TYPE.valueOf(type), params);
            String ret = "jobId: " + jobId;
            responseEntity = new ResponseEntity<>(ret, HttpStatus.CREATED);

        } catch (IllegalArgumentException ie) {
            responseEntity = new ResponseEntity<>(
                    new ErrorMessage(ie.getMessage(), "Flow query job creating error"),
                    HttpStatus.BAD_REQUEST);
        } catch (Throwable t) {
            logger.error("Flow query job creating error", t);
            responseEntity = new ResponseEntity<>(
                    new ErrorMessage(t.getMessage(), "Flow query job creating error"),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }


    @RequestMapping(path = "/{jobId}", method = RequestMethod.GET)
    public
    @ResponseBody
    ResponseEntity<?> getJob(@PathVariable("jobId") String jobId) {
        ResponseEntity<?> responseEntity;

        try {
            Job job = jobService.getJob(jobId);
            responseEntity = new ResponseEntity<>(job, HttpStatus.OK);
        } catch (Throwable t) {
            responseEntity = new ResponseEntity<>(
                    new ErrorMessage(t.getMessage(), "Error when get job " + jobId),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return responseEntity;
    }

    /**
     * @param action {"start", "cancel", "delete}
     * @param jobId
     * @throws JobException
     */
    @RequestMapping(path = "/{jobId}", method = RequestMethod.PUT)
    public void controlJob(@RequestParam("action") String action,
                           @PathVariable("jobId") String jobId) {
        try {
            if (action.equals(ACTION_START)) {
                jobService.start(jobId);
            } else if (action.equals(ACTION_CANCEL)) jobService.cancel(jobId);
            else if (action.equals(ACTION_DELETE)) {
                jobService.delete(jobId);
            } else throw new IllegalArgumentException("Action " + action + " unknown");
        } catch (IllegalArgumentException e) {
            logger.error("Job control argument illegal", e);
        } catch (Throwable t) {
            logger.error("Flow query job action error", t);
        }
    }

}
