package org.jboss.test.messaging.core.base;

import org.jboss.messaging.core.Channel;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * Useful when want to run only tests of a specific subclass.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */
public abstract class NoTestsChannelTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   protected Channel channel;

   // Constructors --------------------------------------------------

   public NoTestsChannelTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all,-aop,-remoting,-security");
      sc.start();
   }

   public void tearDown() throws Exception
   {
      sc.stop();
      sc = null;

      super.tearDown();
   }

   // NO TESTS

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
