package com.hxm.earlgrey.control;

import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by hxm on 17-2-17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = JobsApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class JobTests {
    final static String REQUEST_PATH = "/Jobs";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    public void testJobLifeCycle() throws Exception {
        String name = "testLifeCycle";
        String path = "http://localhost:" + this.port + REQUEST_PATH;
        //String path = "http://localhost:8000/jobs";
        Map<String, String> params = new HashMap<>();
        params.put(Job.PARAM_NAME, "testLifeCycle");
        params.put(Job.PARAM_TYPE, "Test");
        params.put(Job.PARAM_RUNNING_TIME, "2000");
        ResponseEntity<String> jobEntity = testRestTemplate.postForEntity(path, params, String.class);
        logger.info("creating return values: " + jobEntity.getBody());
        assertThat(jobEntity.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String jobId = jobEntity.getBody().split(":")[1].trim();
        Job job = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);

        assertThat(job.getStatus()).isEqualTo(Job.STATUS.Created);

        //start
        testRestTemplate.put(path + "/{jobId}?action=start", null, jobId);
        //cannot determin when to started
        /*job = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);
        assertThat(job.getStatus()).isEqualTo(Job.STATUS.Started);*/

        //do not know the exact time when it finished
        Thread.sleep(5000);

        job = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);
        assertThat(job.getStatus()).isEqualTo(Job.STATUS.End);
    }

    @Test
    public void testCancel() throws Exception {
        String path = "http://localhost:" + this.port + REQUEST_PATH;
        Map<String, String> params = new HashMap<>();
        params.put(Job.PARAM_NAME, "testLifeCycle");
        params.put(Job.PARAM_TYPE, "Test");
        //running 10 seconds
        params.put(Job.PARAM_RUNNING_TIME, "10000");
        ResponseEntity<String> jobEntity = testRestTemplate.postForEntity(path, params, String.class);
        assertThat(jobEntity.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String jobId = jobEntity.getBody().split(":")[1].trim();
        Job jobInfo = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);
        assertThat(jobInfo.getStatus()).isEqualTo(Job.STATUS.Created);

        //start
        testRestTemplate.put(path + "/{jobId}?action=start", params, jobId);
        /*jobInfo = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);
        assertThat(jobInfo.getStatus()).isEqualTo(Job.STATUS.Started);*/

        Thread.sleep(3000);

        //cancel
        testRestTemplate.put(path + "/{jobId}?action=cancel", null, jobId);
        jobInfo = testRestTemplate.getForObject(path + "/{jobId}", Job.class, jobId);
        assertThat(jobInfo.getStatus()).isEqualTo(Job.STATUS.Canceled);
    }
}
