/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.integration.test;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.integration.MinaConnector;
import org.jboss.messaging.core.remoting.integration.MinaService;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public abstract class TestSupport extends TestCase
{
   // Constants -----------------------------------------------------

   public static final int MANY_MESSAGES = 500;

   /**
    * Configurable by system property <code>transport.type</code>, default is
    * TCP
    */
   public final static TransportType TRANSPORT;

   // Attributes ----------------------------------------------------

   Client client;

   private MinaService service;

   public static final int PORT = 9090;

   // Static --------------------------------------------------------

   static
   {
      String transportType = System.getProperty("transport.type", TCP
            .toString());
      TRANSPORT = TransportType.valueOf(transportType);
      info("Default transport is " + TRANSPORT);
   }

   static String reverse(String text)
   {
      // Reverse text
      StringBuffer buf = new StringBuffer(text.length());
      for (int i = text.length() - 1; i >= 0; i--)
      {
         buf.append(text.charAt(i));
      }
      return buf.toString();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   void startServer(int port, TransportType transport) throws Exception
   {
      startServer(port, transport, false);
   }

   void startServer(int port, TransportType transport, boolean useSSL)
         throws Exception
   {
      service = new MinaService("localhost", port);
      service.start();
   }
   
   void stopServer()
   {
      service.stop();
   }

   void startClient(int port, TransportType transport) throws Exception
   {
      startClient(port, transport, false);
   }

   void startClient(int port, TransportType transport, boolean useSSL)
         throws Exception
   {
      client = new Client(new MinaConnector());
      client.connect("localhost", port, transport, useSSL);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static void info(String info)
   {
      System.out.format("### %-50s ###\n", info);
   }

   // Inner classes -------------------------------------------------
}
