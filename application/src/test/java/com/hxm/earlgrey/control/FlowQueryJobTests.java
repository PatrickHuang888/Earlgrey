package com.hxm.earlgrey.control;

import static com.hxm.earlgrey.control.Job.*;
import java.util.HashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by hxm on 17-3-23.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = JobsApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class FlowQueryJobTests {

    final static String REQUEST_PATH = "/Jobs";
    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    public void testFlowQueryJobCreate() {

        Map<String, String> params = new HashMap<>();
        String queryName = "testFlowQuery";
        params.put(PARAM_NAME, queryName);
        params.put(PARAM_TYPE, TYPE.FlowQuery.toString());
        String queryStartTime = "20070726215000";
        params.put(PARAM_STARTTIME, queryStartTime);
        String queryEndTime = "20070726220000";
        params.put(PARAM_ENDTIME, queryEndTime);
        String srcAddr = "171.54.14.189";
        params.put(PARAM_SRCADDRESS, srcAddr);

        String PATH = "http://localhost:" + port + REQUEST_PATH;

        ResponseEntity<String> responseEntity =
                testRestTemplate.postForEntity(PATH, params, String.class);
        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String jobId = responseEntity.getBody().split(":")[1].trim();

        Job job = testRestTemplate.getForObject(PATH + "/{jobId}", Job.class, jobId);
        assertThat(job.getStatus()).isEqualTo(Job.STATUS.Created);
        assertThat(job.getName()).isEqualTo(queryName);
        assertThat(job.getParams().get(PARAM_STARTTIME)).isEqualTo(queryStartTime);
        assertThat(job.getParams().get(PARAM_ENDTIME)).isEqualTo(queryEndTime);
        assertThat(job.getParams().get(PARAM_SRCADDRESS)).isEqualTo(srcAddr);

    }
}
