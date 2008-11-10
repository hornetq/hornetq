/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.client;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.NamingException;
import javax.naming.Reference;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.referenceable.ConnectionFactoryObjectFactory;
import org.jboss.messaging.jms.referenceable.SerializableObjectRefAddr;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class JBossConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
         XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory, Serializable/*
                                                                                                 * , Referenceable
                                                                                                 * http://jira.jboss.org/jira/browse/JBMESSAGING-395
                                                                                                 */
{
   // Constants ------------------------------------------------------------------------------------

   private final static long serialVersionUID = -2810634789345348326L;

   private static final Logger log = Logger.getLogger(JBossConnectionFactory.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private transient volatile ClientSessionFactory sessionFactory;

   private final TransportConfiguration connectorConfig;

   private final TransportConfiguration backupConnectorConfig;

   private final String clientID;

   private final int dupsOKBatchSize;

   private final int transactionBatchSize;

   private final long pingPeriod;

   private final long callTimeout;

   private final int consumerWindowSize;

   private final int consumerMaxRate;

   private final int sendWindowSize;

   private final int producerMaxRate;

   private final boolean blockOnAcknowledge;

   private final boolean blockOnNonPersistentSend;

   private final boolean blockOnPersistentSend;

   private final boolean autoGroup;

   private final int maxConnections;

   // Constructors ---------------------------------------------------------------------------------

   public JBossConnectionFactory(final TransportConfiguration connectorConfig,
                                 final TransportConfiguration backupConnectorConfig,
                                 final long pingPeriod,
                                 final long callTimeout,
                                 final String clientID,
                                 final int dupsOKBatchSize,
                                 final int transactionBatchSize,
                                 final int consumerWindowSize,
                                 final int consumerMaxRate,
                                 final int sendWindowSize,
                                 final int producerMaxRate,
                                 final boolean blockOnAcknowledge,
                                 final boolean blockOnNonPersistentSend,
                                 final boolean blockOnPersistentSend,
                                 final boolean autoGroup,
                                 final int maxConnections)
   {
      this.connectorConfig = connectorConfig;
      this.backupConnectorConfig = backupConnectorConfig;
      this.clientID = clientID;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.transactionBatchSize = transactionBatchSize;
      this.pingPeriod = pingPeriod;
      this.callTimeout = callTimeout;
      this.consumerMaxRate = consumerMaxRate;
      this.consumerWindowSize = consumerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
   }

   // ConnectionFactory implementation -------------------------------------------------------------

   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }

   public Connection createConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_GENERIC_CONNECTION);
   }

   // QueueConnectionFactory implementation --------------------------------------------------------

   public QueueConnection createQueueConnection() throws JMSException
   {
      return createQueueConnection(null, null);
   }

   public QueueConnection createQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_QUEUE_CONNECTION);
   }

   // TopicConnectionFactory implementation --------------------------------------------------------

   public TopicConnection createTopicConnection() throws JMSException
   {
      return createTopicConnection(null, null);
   }

   public TopicConnection createTopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_TOPIC_CONNECTION);
   }

   // XAConnectionFactory implementation -----------------------------------------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_GENERIC_CONNECTION);
   }

   // XAQueueConnectionFactory implementation ------------------------------------------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return createXAQueueConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_QUEUE_CONNECTION);
   }

   // XATopicConnectionFactory implementation ------------------------------------------------------

   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return createXATopicConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_TOPIC_CONNECTION);
   }

   // Referenceable implementation -----------------------------------------------------------------

   public Reference getReference() throws NamingException
   {
      return new Reference(this.getClass().getCanonicalName(),
                           new SerializableObjectRefAddr("JBM-CF", this),
                           ConnectionFactoryObjectFactory.class.getCanonicalName(),
                           null);
   }

   // Public ---------------------------------------------------------------------------------------

   public TransportConfiguration getConnectorFactory()
   {
      return connectorConfig;
   }

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public String getClientID()
   {
      return clientID;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public int getProducerWindowSize()
   {
      return sendWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected synchronized JBossConnection createConnectionInternal(final String username,
                                                                   final String password,
                                                                   final boolean isXA,
                                                                   final int type) throws JMSException
   {     
      if (sessionFactory == null)
      {
         sessionFactory = new ClientSessionFactoryImpl(connectorConfig,
                                                       backupConnectorConfig,
                                                       pingPeriod,
                                                       callTimeout,
                                                       consumerWindowSize,
                                                       consumerMaxRate,
                                                       sendWindowSize,
                                                       producerMaxRate,
                                                       blockOnAcknowledge,
                                                       blockOnNonPersistentSend,
                                                       blockOnPersistentSend,
                                                       autoGroup,
                                                       maxConnections,
                                                       DEFAULT_ACK_BATCH_SIZE);

      }

      if (username != null)
      {
         // Since core has no connection concept, we need to create a session in order to authenticate at this time

         ClientSession sess = null;

         try
         {
            sess = sessionFactory.createSession(username, password, false, false, false, false, 0);
         }
         catch (MessagingException e)
         {
            throw JMSExceptionHelper.convertFromMessagingException(e);
         }
         finally
         {
            if (sess != null)
            {
               try
               {
                  sess.close();
               }
               catch (Throwable ignore)
               {
               }
            }
         }
      }

      return new JBossConnection(username,
                                 password,
                                 type,
                                 clientID,
                                 dupsOKBatchSize,
                                 transactionBatchSize,
                                 sessionFactory);
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
