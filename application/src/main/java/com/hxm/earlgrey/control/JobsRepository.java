package com.hxm.earlgrey.control;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by hxm on 17-2-15.
 */
@Repository
public interface JobsRepository extends MongoRepository<Job, String>{
}
