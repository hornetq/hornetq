package org.hornetq.jms.tests;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.MessageLogger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/17/12
 */

@MessageLogger(projectCode = "HQTEST")
public interface JmsTestLogger extends BasicLogger
{
   /**
    * The jms test logger.
    */
   JmsTestLogger LOGGER = Logger.getMessageLogger(JmsTestLogger.class, JmsTestLogger.class.getPackage().getName());
}
