/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting;

import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.ClientKeepAliveHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ClientKeepAliveHandlerTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPing() throws Exception
   {
      ClientKeepAliveHandler handler = new ClientKeepAliveHandler();

      Ping ping = new Ping(randomLong());
      Pong pong = handler.ping(ping);
      
      assertEquals(ping.getSessionID(), pong.getSessionID());
      assertEquals(ping.getResponseTargetID(), pong.getTargetID());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
