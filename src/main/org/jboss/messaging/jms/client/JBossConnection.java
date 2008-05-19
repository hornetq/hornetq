/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicConnection;
import javax.jms.XATopicSession;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossConnection implements
    Connection, QueueConnection, TopicConnection,
    XAConnection, XAQueueConnection, XATopicConnection
{
   // Constants ------------------------------------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JBossConnection.class);

   static final int TYPE_GENERIC_CONNECTION = 0;
   
   static final int TYPE_QUEUE_CONNECTION = 1;
   
   static final int TYPE_TOPIC_CONNECTION = 2;
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final ClientConnection connection;
   
   private final int connectionType;
   
   private final int dupsOKBatchSize;
   
   private volatile ExceptionListener exceptionListener;
   
   private volatile boolean justCreated = true;      
   
   private volatile ConnectionMetaData metaData;
   
   private String clientID;
              
   // Constructors ---------------------------------------------------------------------------------

   public JBossConnection(final ClientConnection connection, final int connectionType,
                          final String clientID, final int dupsOKBatchSize)
   {
      this.connection = connection;
      
      this.connectionType = connectionType;
      
      this.clientID = clientID;
      
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   // Connection implementation --------------------------------------------------------------------

   public Session createSession(final boolean transacted, final int acknowledgeMode) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false, TYPE_GENERIC_CONNECTION, false);
   }
   
   public String getClientID() throws JMSException
   {
      checkClosed();
            
      justCreated = false;
      
      return clientID;
   }

   public void setClientID(final String clientID) throws JMSException
   {
      checkClosed();
      
      if (this.clientID != null)
      {
         throw new IllegalStateException("Client id has already been set");
      }
      
      if (!justCreated)
      {
         throw new IllegalStateException("setClientID can only be called directly after the connection is created");
      }

      this.clientID = clientID;

      justCreated = false;
   }
   
   public ConnectionMetaData getMetaData() throws JMSException
   {
      checkClosed();
    
      justCreated = false;

      if (metaData == null)
      {
         metaData = new JBossConnectionMetaData(connection.getServerVersion());
      }

      return metaData;
   }
      
   public ExceptionListener getExceptionListener() throws JMSException
   {
      justCreated = false;
      
      return exceptionListener;
   }

   public void setExceptionListener(final ExceptionListener listener) throws JMSException
   {
      try
      {
         if (listener == null)
         {
            connection.setRemotingSessionListener(null);                 
         }
         else
         {
            connection.setRemotingSessionListener(new JMSFailureListener());
         }
         
         exceptionListener = listener;
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
      
      justCreated = false;
   }

   public void start() throws JMSException
   {
      try
      {
         connection.start();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
      
      justCreated = false;
   }

   public void stop() throws JMSException
   {
      try
      {
         connection.stop();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
      
      justCreated = false;
   }

   public void close() throws JMSException
   {

      try
      {
         connection.close();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
   }

   public ConnectionConsumer createConnectionConsumer(final Destination destination,
                                                      final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException
   {
      //TODO
      return null;
   }

   public ConnectionConsumer createDurableConnectionConsumer(final Topic topic,
                                                             final String subscriptionName,
                                                             final String messageSelector,
                                                             final ServerSessionPool sessionPool,
                                                             final int maxMessages) throws JMSException
   {
      // As spec. section 4.11
      if (connectionType == TYPE_QUEUE_CONNECTION)
      {
         String msg = "Cannot create a durable connection consumer on a QueueConnection";
         throw new javax.jms.IllegalStateException(msg);
      }
     
      //TODO
      return null;
   }

   // QueueConnection implementation ---------------------------------------------------------------

   public QueueSession createQueueSession(final boolean transacted,
                                          final int acknowledgeMode) throws JMSException
   {
       return createSessionInternal(transacted, acknowledgeMode, false,
                                    JBossSession.TYPE_QUEUE_SESSION, false);
   }

   public ConnectionConsumer createConnectionConsumer(final Queue queue, final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException
   {
      //TODO
      
      return null;
   }

   // TopicConnection implementation ---------------------------------------------------------------

   public TopicSession createTopicSession(final boolean transacted,
                                          final int acknowledgeMode) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false,
                                   JBossSession.TYPE_TOPIC_SESSION, false);
   }
   
   public ConnectionConsumer createConnectionConsumer(final Topic topic, final String messageSelector,
                                                      final ServerSessionPool sessionPool,
                                                      final int maxMessages) throws JMSException
   {
      //TODO
      
      return null;
   }

   // XAConnection implementation ------------------------------------------------------------------

   public XASession createXASession() throws JMSException
   {
       return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                    JBossSession.TYPE_GENERIC_SESSION, false);
   }
   
   // XAQueueConnection implementation -------------------------------------------------------------

   public XAQueueSession createXAQueueSession() throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_QUEUE_SESSION, false);

   }
   

   // XATopicConnection implementation -------------------------------------------------------------

   public XATopicSession createXATopicSession() throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_TOPIC_SESSION, false);

   }
   
   // Public ---------------------------------------------------------------------------------------

   // We provide some overloaded createSession methods to allow the value of cacheProducers to be specified
   
   public Session createSession(final boolean transacted, final int acknowledgeMode,
   		                       final boolean cacheProducers) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false, TYPE_GENERIC_CONNECTION, cacheProducers);
   }
   
   public TopicSession createTopicSession(final boolean transacted,
         final int acknowledgeMode, final boolean cacheProducers) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false,
                                   JBossSession.TYPE_TOPIC_SESSION, cacheProducers);
   }

   public XASession createXASession(final boolean cacheProducers) throws JMSException
   {
       return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                    JBossSession.TYPE_GENERIC_SESSION, cacheProducers);
   }
   
   public XAQueueSession createXAQueueSession(final boolean cacheProducers) throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_QUEUE_SESSION, cacheProducers);
   }
   
   public XATopicSession createXATopicSession(final boolean cacheProducers) throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_TOPIC_SESSION, cacheProducers);
   }


   public ClientConnection getConnection()
   {
      return connection;
   }

   public String toString()
   {
      return "JBossConnection->" + connection;
   }
   
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected JBossSession createSessionInternal(final boolean transacted, int acknowledgeMode,
                                                final boolean isXA, final int type, final boolean cacheProducers) throws JMSException
   {
      if (transacted)
      {
         acknowledgeMode = Session.SESSION_TRANSACTED;
      }
      
      try
      {
         ClientSession session;

      	if (acknowledgeMode == Session.SESSION_TRANSACTED)
      	{
      	   session =
               connection.createClientSession(isXA, false, false, -1, false, cacheProducers);
      	}
      	else if (acknowledgeMode == Session.AUTO_ACKNOWLEDGE)
         {
      	   session = connection.createClientSession(isXA, true, true, 1);
         }
         else if (acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            session =
               connection.createClientSession(isXA, true, true, dupsOKBatchSize, false, cacheProducers);
         }
         else if (acknowledgeMode == Session.CLIENT_ACKNOWLEDGE)
         {
            session =
               connection.createClientSession(isXA, true, false, -1, false, cacheProducers);
         }         
         else
         {
         	throw new IllegalArgumentException("Invalid ackmode: " + acknowledgeMode);
         }

         justCreated = false;
         
         return new JBossSession(this, transacted, isXA, acknowledgeMode, session, type);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }            
   }

   // Private --------------------------------------------------------------------------------------
   
   private void checkClosed() throws JMSException
   {
      if (connection.isClosed())
      {
         throw new IllegalStateException("Connection is closed");
      }
   }

   // Inner classes --------------------------------------------------------------------------------
   
   private class JMSFailureListener implements RemotingSessionListener
   {
      public void sessionDestroyed(long sessionID, MessagingException me)
      {
         if (me == null)
            return;
         
         JMSException je = new JMSException(me.toString());
         
         je.initCause(me);
         
         exceptionListener.onException(je);
      }
      
   }
}
