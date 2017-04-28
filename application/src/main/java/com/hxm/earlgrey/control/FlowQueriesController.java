package com.hxm.earlgrey.control;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * Created by hxm on 17-2-8.
 */
@Controller
@RequestMapping("/FlowQueries")
public class FlowQueriesController {
    @Autowired
    private MongoTemplate mongoTemplate;


    @RequestMapping(path = "/{jobId}/results", method = RequestMethod.GET)
    public ResponseEntity<?> getQueryResult(@PathVariable("jobId") String jobId,
                                            @RequestParam("offset") int offset,
                                            @RequestParam("limit") int limit) {
        ResponseEntity responseEntity;
        try {
            DBCollection collection = mongoTemplate.getCollection("FlowQuery-" + jobId);
            List<DBObject> objs = collection.find().skip(offset).limit(limit).toArray();
            List<String> ret = new LinkedList<>();
            for (DBObject dbo : objs) {
                ret.add(dbo.toString());
            }
            responseEntity = new ResponseEntity(ret, HttpStatus.OK);
        } catch (Throwable t) {
            responseEntity = new ResponseEntity(
                    new ErrorMessage(t.getMessage(), "Retrive flow query result error"),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return responseEntity;
    }


}


