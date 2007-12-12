/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.integration.test.TestSupport.PORT;

import java.io.IOException;

import javax.jms.IllegalStateException;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.integration.test.TestSupport;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ClientTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConnected() throws Exception
   {
      Client client = new Client(new NIOConnectorAdapter()
      {
         private boolean connected = false;

         @Override
         public NIOSession connect(String host, int port,
               TransportType transport) throws IOException
         {
            connected = true;
            return new NIOSessionAdapter()
            {
               @Override
               public boolean isConnected()
               {
                  return connected;
               }
            };
         }

         @Override
         public boolean disconnect()
         {
            boolean wasConnected = connected;
            connected = false;
            return wasConnected;
         }
      });

      assertFalse(client.isConnected());

      client.connect("localhost", TestSupport.PORT, TCP);
      assertTrue(client.isConnected());

      assertTrue(client.disconnect());
      assertFalse(client.isConnected());
      assertFalse(client.disconnect());
   }

   public void testConnectionFailure() throws Exception
   {
      Client client = new Client(new NIOConnectorAdapter()
      {
         @Override
         public NIOSession connect(String host, int port,
               TransportType transport) throws IOException
         {
            throw new IOException("connection exception");
         }
      });

      try
      {
         client.connect("localhost", PORT, TCP);
         fail("connection must fail");
      } catch (IOException e)
      {
      }
   }

   public void testSessionID() throws Exception
   {
      Client client = new Client(new NIOConnectorAdapter()
      {
         @Override
         public NIOSession connect(String host, int port,
               TransportType transport) throws IOException
         {
            return new NIOSessionAdapter()
            {
               @Override
               public long getID()
               {
                  return System.currentTimeMillis();
               }

               @Override
               public boolean isConnected()
               {
                  return true;
               }
            };
         }
      });
      assertNull(client.getSessionID());
      client.connect("localhost", PORT, TCP);
      assertNotNull(client.getSessionID());
      client.disconnect();
      assertNull(client.getSessionID());
   }

   public void testURI() throws Exception
   {
      Client client = new Client(new NIOConnectorAdapter()
      {
         private boolean connected = false;

         @Override
         public NIOSession connect(String host, int port,
               TransportType transport) throws IOException
         {
            connected = true;
            return super.connect(host, port, transport);
         }

         @Override
         public boolean disconnect()
         {
            connected = false;
            return true;
         }

         @Override
         public String getServerURI()
         {
            if (!connected)
               return null;
            else
               return "tcp://localhost:" + PORT;
         }
      });
      assertNull(client.getURI());
      client.connect("localhost", PORT, TCP);
      assertNotNull(client.getURI());
      client.disconnect();
      assertNull(client.getURI());
   }

   public void testCanNotSendPacketIfNotConnected() throws Exception
   {
      Client client = new Client(new NIOConnectorAdapter()
      {
         @Override
         public NIOSession connect(String host, int port,
               TransportType transport) throws IOException
         {
            return null;
         }
      });

      try
      {
         client.sendOneWay(new NullPacket());
         fail("can not send a packet if the dispatcher is not connected");
      } catch (IllegalStateException e)
      {

      }
   }
}
