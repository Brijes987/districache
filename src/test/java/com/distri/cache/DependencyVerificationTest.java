package com.distri.cache;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verification test to ensure all dependencies are correctly configured.
 */
class DependencyVerificationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(DependencyVerificationTest.class);
    
    @Test
    void verifyJUnit5Works() {
        logger.info("JUnit 5 test running successfully");
        assertNotNull(logger);
    }
    
    @Test
    void verifySLF4JWorks() {
        Logger testLogger = LoggerFactory.getLogger("test");
        assertNotNull(testLogger);
        testLogger.info("SLF4J logging is working correctly");
    }
}
