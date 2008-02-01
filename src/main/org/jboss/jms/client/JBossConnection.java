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
package org.jboss.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
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

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientSession;

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

   static final int TYPE_GENERIC_CONNECTION = 0;
   static final int TYPE_QUEUE_CONNECTION = 1;
   static final int TYPE_TOPIC_CONNECTION = 2;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   protected ClientConnection connection;
   private int connectionType;

   // Constructors ---------------------------------------------------------------------------------

   public JBossConnection(ClientConnection delegate, int connectionType)
   {
      this.connection = delegate;
      this.connectionType = connectionType;
   }

   // Connection implementation --------------------------------------------------------------------

   public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false, TYPE_GENERIC_CONNECTION);
   }

   public String getClientID() throws JMSException
   {
      return connection.getClientID();
   }

   public void setClientID(String clientID) throws JMSException
   {
      connection.setClientID(clientID);
   }

   public ConnectionMetaData getMetaData() throws JMSException
   {
      return connection.getConnectionMetaData();
   }

   public ExceptionListener getExceptionListener() throws JMSException
   {
      return connection.getExceptionListener();
   }

   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      connection.setExceptionListener(listener);
   }

   public void start() throws JMSException
   {
      connection.start();
   }

   public void stop() throws JMSException
   {
      connection.stop();
   }

   public void close() throws JMSException
   {
      try
      {
         connection.closing();
         connection.close();
      } finally
      {
         // FIXME regardless of the pb when closing/close the connection, we must ensure
         // the remoting connection is properly stopped
         connection.getRemotingConnection().stop();
      }
   }

   public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                      String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
   {
      return connection.
         createConnectionConsumer(destination, null, messageSelector, sessionPool, maxMessages);
   }

   public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                             String subscriptionName,
                                                             String messageSelector,
                                                             ServerSessionPool sessionPool,
                                                             int maxMessages) throws JMSException
   {
      // As spec. section 4.11
      if (connectionType == TYPE_QUEUE_CONNECTION)
      {
         String msg = "Cannot create a durable connection consumer on a QueueConnection";
         throw new javax.jms.IllegalStateException(msg);
      }
      return connection.createConnectionConsumer(topic, subscriptionName, messageSelector,
                                               sessionPool, maxMessages);
   }

   // QueueConnection implementation ---------------------------------------------------------------

   public QueueSession createQueueSession(boolean transacted,
                                          int acknowledgeMode) throws JMSException
   {
       return createSessionInternal(transacted, acknowledgeMode, false,
                                    JBossSession.TYPE_QUEUE_SESSION);
   }

   public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
    {
      return connection.
         createConnectionConsumer(queue, null, messageSelector, sessionPool, maxMessages);
    }

   // TopicConnection implementation ---------------------------------------------------------------

   public TopicSession createTopicSession(boolean transacted,
                                          int acknowledgeMode) throws JMSException
   {
      return createSessionInternal(transacted, acknowledgeMode, false,
                                   JBossSession.TYPE_TOPIC_SESSION);
   }

   public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector,
                                                      ServerSessionPool sessionPool,
                                                      int maxMessages) throws JMSException
   {
      return connection.
         createConnectionConsumer(topic, null, messageSelector, sessionPool, maxMessages);
   }

   // XAConnection implementation ------------------------------------------------------------------

   public XASession createXASession() throws JMSException
   {
       return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                    JBossSession.TYPE_GENERIC_SESSION);
   }

   // XAQueueConnection implementation -------------------------------------------------------------

   public XAQueueSession createXAQueueSession() throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_QUEUE_SESSION);

   }

   // XATopicConnection implementation -------------------------------------------------------------

   public XATopicSession createXATopicSession() throws JMSException
   {
      return createSessionInternal(true, Session.SESSION_TRANSACTED, true,
                                   JBossSession.TYPE_TOPIC_SESSION);

   }

   // Public ---------------------------------------------------------------------------------------

   /* For testing only */
   public String getRemotingClientSessionID()
   {
      return connection.getRemotingConnection().getSessionID();
   }

   public ClientConnection getDelegate()
   {
      return connection;
   }

   /**
    * Convenience method.
    */
   public int getServerID()
   {
      
      return connection.getServerID(); 
   }

   public String toString()
   {
      return "JBossConnection->" + connection;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected JBossSession createSessionInternal(boolean transacted, int acknowledgeMode,
                                                boolean isXA, int type) throws JMSException
   {
      if (transacted)
      {
         acknowledgeMode = Session.SESSION_TRANSACTED;
      }

      ClientSession session =  connection.createClientSession(transacted, acknowledgeMode, isXA);
      
      return new JBossSession(transacted, acknowledgeMode, session, type);
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
