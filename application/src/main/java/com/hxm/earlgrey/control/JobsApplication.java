package com.hxm.earlgrey.control;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by hxm on 17-2-8.
 */
@SpringBootApplication
public class JobsApplication {

    @Value("${projectHome}")
    public String projectHome;

    public static String TEST_JOB_CLASS = "com.hxm.earlgrey.jobs.com.hxm.earlgrey.common.TestJob";
    public String TEST_JARFILE = projectHome + "/testjob/target/testjob-0.1.jar";
    //public static String TEST_COMMAND="java -classpath "+APP_HOME +"/target/jobs-com.hxm.earlgrey.common-0.1.jar ";


    public static void main(String[] args) {
        SpringApplication.run(JobsApplication.class, args);
    }
}
