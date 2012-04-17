package org.hornetq.tests.unit;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.MessageLogger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/17/12
 */

@MessageLogger(projectCode = "HQTEST")
public interface UnitTestLogger extends BasicLogger
{
   /**
    * The unit test logger.
    */
   UnitTestLogger LOGGER = Logger.getMessageLogger(UnitTestLogger.class, UnitTestLogger.class.getPackage().getName());

}
