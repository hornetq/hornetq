/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Client
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(Client.class);

   // Attributes ----------------------------------------------------

   private ServerLocator serverLocator;

   private final NIOConnector connector;

   private NIOSession session;

   // By default, a blocking request will timeout after 5 seconds
   private int blockingRequestTimeout = 5;
   private TimeUnit blockingRequestTimeUnit = SECONDS;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Client(NIOConnector connector, ServerLocator locator)
   {
      assert connector != null;
      assert locator != null;
      
      this.connector = connector;
      this.serverLocator = locator;
   }

   // Public --------------------------------------------------------

   public void connect() throws Exception
   {
      this.session = connector.connect();
   }

   public boolean disconnect() throws Exception
   {
      if (session == null)
      {
         return false;         
      }
      session = null;
      return true;
   }

   public String getSessionID()
   {
      if (session == null || !session.isConnected())
      {
         return null;
      }
      return Long.toString(session.getID());
   }

   public void sendOneWay(AbstractPacket packet) throws JMSException
   {
      assert packet != null;
      checkConnected();

      session.write(packet);
   }

   public AbstractPacket sendBlocking(AbstractPacket packet)
         throws IOException, JMSException
   {
      assert packet != null;
      checkConnected();

      try
      {
         AbstractPacket response = (AbstractPacket) session.writeAndBlock(packet, 
               blockingRequestTimeout, blockingRequestTimeUnit);
         return response;
      } catch (Throwable t)
      {
         IOException ioe = new IOException();
         ioe.initCause(t);
         throw ioe;
      }
   }

   public void setBlockingRequestTimeout(int timeout, TimeUnit unit)
   {
      this.blockingRequestTimeout = timeout;
      this.blockingRequestTimeUnit = unit;
   }

   public void addConnectionListener(
         final ConsolidatedRemotingConnectionListener listener)
   {
      connector.addConnectionListener(listener);
   }

   public void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
      connector.removeConnectionListener(listener);
   }

   public boolean isConnected()
   {
      if (session == null)
         return false;
      else
         return session.isConnected();
   }

   public String getURI()
   {
      return connector.getServerURI();
   }

   @Override
   public String toString()
   {
      return "Client[session=" + session + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkConnected() throws JMSException
   {
      if (session == null)
      {
         throw new IllegalStateException("Client " + this
               + " is not connected.");
      }
      if (!session.isConnected())
      {
         throw new MessagingNetworkFailureException("Client " + this
               + " is not connected.");
      }
   }

   // Inner classes -------------------------------------------------
}
