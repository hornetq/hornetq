/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.TimeUnit;

import org.jboss.jms.client.api.FailureListener;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

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

   private final NIOConnector connector;

   private NIOSession session;

   // By default, a blocking request will timeout after 5 seconds
   private int blockingRequestTimeout = 5;
   
   private TimeUnit blockingRequestTimeUnit = SECONDS;
   
   private FailureListener failureListener;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClientImpl(NIOConnector connector, RemotingConfiguration remotingConfig)
   {
      assert connector != null;
      assert remotingConfig != null;
      
      this.connector = connector;
      setBlockingRequestTimeout(remotingConfig.getTimeout(), SECONDS);
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

   public AbstractPacket send(AbstractPacket packet, boolean oneWay)
         throws Exception
   {
      assert packet != null;
      checkConnected();
      packet.setOneWay(oneWay);

      if (oneWay)
      {
         sendOneWay(packet);
         return null;
      } else 
      {
         return sendBlocking(packet);
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

   public void setFailureListener(final FailureListener listener)
   {
      if (listener == null && failureListener != null)
      {
         connector.removeFailureListener(failureListener);
         
         failureListener = null;
      }
      else
      {
         if (failureListener != null)
         {
            throw new IllegalStateException("Cannot set FailureListener - already has one set");
         }
         connector.addFailureListener(listener);
         
         failureListener = listener;
      }
   }

   public FailureListener getFailureListener()
   {
      return failureListener;
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

   private void checkConnected() throws MessagingException
   {
      if (session == null)
      {
         throw new IllegalStateException("Client " + this
               + " is not connected.");
      }
      if (!session.isConnected())
      {
         throw new MessagingException(MessagingException.NOT_CONNECTED);
      }
   }
   
   private void sendOneWay(AbstractPacket packet) throws Exception
   {
      session.write(packet);
   }

   private AbstractPacket sendBlocking(AbstractPacket packet) throws Exception
   {
      AbstractPacket response = (AbstractPacket) session.writeAndBlock(packet, 
                                               blockingRequestTimeout, blockingRequestTimeUnit);
      return response;
   }

   // Inner classes -------------------------------------------------
}
