package com.apple.springboot;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "aws.region=us-east-1"
})
class SpringbootApplicationTests {

    @Test

    void contextLoads() {
        // This test simply ensures that the Spring application context can load successfully.
    }

}