/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.referenceable.ConnectionFactoryObjectFactory;
import org.jboss.messaging.jms.referenceable.SerializableObjectRefAddr;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossConnectionFactory implements
   ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
   XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory,
   Serializable/*, Referenceable http://jira.jboss.org/jira/browse/JBMESSAGING-395*/
{
   // Constants ------------------------------------------------------------------------------------
   
   private final static long serialVersionUID = -2810634789345348326L;
   
   private static final Logger log = Logger.getLogger(JBossConnectionFactory.class);

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   private transient ClientSessionFactory sessionFactory;
   
   private final String clientID;
   
   private final int dupsOKBatchSize;

   private final Location location;

   private final ConnectionParams connectionParams;

   private final int defaultConsumerWindowSize;

   private final int defaultConsumerMaxRate;

   private final int defaultProducerWindowSize;

   private final int defaultProducerMaxRate;
   
   private final boolean defaultBlockOnAcknowledge;
   
   private final boolean defaultSendNonPersistentMessagesBlocking;
   
   private final boolean defaultSendPersistentMessagesBlocking;

   // Constructors ---------------------------------------------------------------------------------
   
   public JBossConnectionFactory(final String clientID,
   		                        final int dupsOKBatchSize,
                                 final Location location,
                                 final ConnectionParams connectionParams,                         
                                 final int defaultConsumerWindowSize,
                                 final int defaultConsumerMaxRate,
                                 final int defaultProducerWindowSize,
                                 final int defaultProducerMaxRate,
                                 final boolean defaultBlockOnAcknowledge,
                                 final boolean defaultSendNonPersistentMessagesBlocking,
                                 final boolean defaultSendPersistentMessagesBlocking)
   {
      this.clientID = clientID;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.location = location;
      this.connectionParams = connectionParams;
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      this.defaultBlockOnAcknowledge = defaultBlockOnAcknowledge;
      this.defaultSendNonPersistentMessagesBlocking = defaultSendNonPersistentMessagesBlocking;
      this.defaultSendPersistentMessagesBlocking = defaultSendPersistentMessagesBlocking;
   }
   
   // ConnectionFactory implementation -------------------------------------------------------------
   
   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }
   
   public Connection createConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false,
                                      JBossConnection.TYPE_GENERIC_CONNECTION);
   }
   
   // QueueConnectionFactory implementation --------------------------------------------------------
   
   public QueueConnection createQueueConnection() throws JMSException
   {
      return createQueueConnection(null, null);
   }
   
   public QueueConnection createQueueConnection(final String username, final String password)
      throws JMSException
   {
      return createConnectionInternal(username, password, false,
                                      JBossConnection.TYPE_QUEUE_CONNECTION);
   }
   
   // TopicConnectionFactory implementation --------------------------------------------------------
   
   public TopicConnection createTopicConnection() throws JMSException
   {
      return createTopicConnection(null, null);
   }
   
   public TopicConnection createTopicConnection(final String username, final String password)
      throws JMSException
   {
      return createConnectionInternal(username, password, false,
                                      JBossConnection.TYPE_TOPIC_CONNECTION);
   }
   
   // XAConnectionFactory implementation -----------------------------------------------------------
   
   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }
   
   public XAConnection createXAConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true,
                                      JBossConnection.TYPE_GENERIC_CONNECTION);
   }
   
   // XAQueueConnectionFactory implementation ------------------------------------------------------
   
   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return createXAQueueConnection(null, null);
   }
   
   public XAQueueConnection createXAQueueConnection(final String username, final String password)
      throws JMSException
   {
      return createConnectionInternal(username, password, true,
                                      JBossConnection.TYPE_QUEUE_CONNECTION);
   }
   
   // XATopicConnectionFactory implementation ------------------------------------------------------
   
   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return createXATopicConnection(null, null);
   }
   
   public XATopicConnection createXATopicConnection(final String username, final String password)
      throws JMSException
   {
      return createConnectionInternal(username, password, true,
                                      JBossConnection.TYPE_TOPIC_CONNECTION);
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
   
   public String getClientID()
   {
      return clientID;
   }
   
   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }
   
   public Location getLocation()
   {
      return location;
   }

   public int getDefaultConsumerWindowSize()
   {
      return defaultConsumerWindowSize;
   }

   public int getDefaultConsumerMaxRate()
   {
      return defaultConsumerMaxRate;
   }

   public int getDefaultProducerWindowSize()
   {
      return defaultProducerWindowSize;
   }

   public int getDefaultProducerMaxRate()
   {
      return defaultProducerMaxRate;
   }

   public boolean isDefaultBlockOnAcknowledge()
   {
      return defaultBlockOnAcknowledge;
   }

   public boolean isDefaultSendNonPersistentMessagesBlocking()
   {
      return defaultSendNonPersistentMessagesBlocking;
   }

   public boolean isDefaultSendPersistentMessagesBlocking()
   {
      return defaultSendPersistentMessagesBlocking;
   }
      
   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   protected JBossConnection createConnectionInternal(final String username, final String password,
                                                      final boolean isXA, final int type)
      throws JMSException
   {
//      try
//      {
         synchronized (this)
         {
            if (sessionFactory == null)
            {
               sessionFactory = new ClientSessionFactoryImpl(
                     location,
                     connectionParams,
                     defaultConsumerWindowSize,
                     defaultConsumerMaxRate,
                     defaultProducerWindowSize,
                     defaultProducerMaxRate,
                     defaultBlockOnAcknowledge,
                     defaultSendNonPersistentMessagesBlocking,
                     defaultSendPersistentMessagesBlocking);
   
            }
         }
           
         return new JBossConnection(username, password, type, clientID, dupsOKBatchSize, sessionFactory);
//      }
//      catch (MessagingException e)
//      {
//         throw JMSExceptionHelper.convertFromMessagingException(e);     
//      }
   }

  
   // Private --------------------------------------------------------------------------------------
      
   // Inner classes --------------------------------------------------------------------------------
}
