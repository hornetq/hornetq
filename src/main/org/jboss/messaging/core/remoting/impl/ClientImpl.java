/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ClientImpl implements Client
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(ClientImpl.class);

   // Attributes ----------------------------------------------------

   private ServerLocator serverLocator;

   private final NIOConnector connector;

   private NIOSession session;

   // By default, a blocking request will timeout after 5 seconds
   private int blockingRequestTimeout = 5;
   private TimeUnit blockingRequestTimeUnit = SECONDS;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientImpl(NIOConnector connector, ServerLocator locator)
   {
      assert connector != null;
      assert locator != null;
      
      this.connector = connector;
      this.serverLocator = locator;
      if (locator.getParameters().containsKey("timeout"))
      {
         int timeout = Integer.parseInt(locator.getParameters().get("timeout"));
         setBlockingRequestTimeout(timeout, SECONDS);
      }
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#connect()
    */
   public void connect() throws Exception
   {
      this.session = connector.connect();
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#disconnect()
    */
   public boolean disconnect() throws Exception
   {
      if (session == null)
      {
         return false;         
      }
      session = null;
      return true;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#getSessionID()
    */
   public String getSessionID()
   {
      if (session == null || !session.isConnected())
      {
         return null;
      }
      return session.getID();
   }

   public void sendOneWay(AbstractPacket packet) throws JMSException
   {
      assert packet != null;
      checkConnected();
      packet.setOneWay(true);
      
      session.write(packet);
   }

   public AbstractPacket sendBlocking(AbstractPacket packet)
         throws IOException, JMSException
   {
      assert packet != null;
      checkConnected();

      packet.setOneWay(false);
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

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#setBlockingRequestTimeout(int, java.util.concurrent.TimeUnit)
    */
   public void setBlockingRequestTimeout(int timeout, TimeUnit unit)
   {
      this.blockingRequestTimeout = timeout;
      this.blockingRequestTimeUnit = unit;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#addConnectionListener(org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener)
    */
   public void addConnectionListener(
         final ConsolidatedRemotingConnectionListener listener)
   {
      connector.addConnectionListener(listener);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#removeConnectionListener(org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener)
    */
   public void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener)
   {
      connector.removeConnectionListener(listener);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#isConnected()
    */
   public boolean isConnected()
   {
      if (session == null)
         return false;
      else
         return session.isConnected();
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.Client#getURI()
    */
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
