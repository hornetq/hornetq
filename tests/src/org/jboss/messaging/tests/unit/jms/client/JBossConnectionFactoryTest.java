/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.client;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;

import junit.framework.TestCase;

import org.easymock.classextension.EasyMock;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossConnectionFactoryTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCreateConnection() throws Exception
   {
      doCreateConnection(Connection.class, new ConnectionCreation()
      {
         Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createConnection();
         }
      });
   }

   public void testCreateConnectionWithCredentials() throws Exception
   {
      doCreateConnectionWithCredentials(Connection.class, randomString(),
            randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createConnection(user, password);
               }
            });
   }

   public void testCreateQueueConnection() throws Exception
   {
      doCreateConnection(QueueConnection.class, new ConnectionCreation()
      {
         public Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createQueueConnection();
         }
      });
   }

   public void testCreateQueueConnectionWithCredentials() throws Exception
   {
      doCreateConnectionWithCredentials(QueueConnection.class, randomString(),
            randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createQueueConnection(user, password);
               }
            });
   }

   public void testCreateTopicConnection() throws Exception
   {
      doCreateConnection(TopicConnection.class, new ConnectionCreation()
      {
         public Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createTopicConnection();
         }
      });
   }

   public void testCreateTopicConnectionWithUserPassword() throws Exception
   {
      doCreateConnectionWithCredentials(TopicConnection.class, randomString(),
            randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createTopicConnection(user, password);
               }
            });
   }

   public void testCreateXAConnection() throws Exception
   {
      doCreateConnection(XAConnection.class, new ConnectionCreation()
      {
         Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createXAConnection();
         }
      });
   }

   public void testCreateXAConnectionWithCredentials() throws Exception
   {
      doCreateConnectionWithCredentials(XAConnection.class, randomString(),
            randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createXAConnection(user, password);
               }
            });
   }

   public void testCreateXAQueueConnection() throws Exception
   {
      doCreateConnection(XAQueueConnection.class, new ConnectionCreation()
      {
         public Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createXAQueueConnection();
         }
      });
   }

   public void testCreateXAQueueConnectionWithCredentials() throws Exception
   {
      doCreateConnectionWithCredentials(XAQueueConnection.class,
            randomString(), randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createXAQueueConnection(user, password);
               }
            });
   }

   public void testCreateXATopicConnection() throws Exception
   {
      doCreateConnection(XATopicConnection.class, new ConnectionCreation()
      {
         public Connection createConnection(JBossConnectionFactory factory)
               throws Exception
         {
            return factory.createXATopicConnection();
         }
      });
   }

   public void testCreateXATopicConnectionWithUserPassword() throws Exception
   {
      doCreateConnectionWithCredentials(XATopicConnection.class,
            randomString(), randomString(), new ConnectionCreation()
            {
               Connection createConnection(JBossConnectionFactory factory,
                     String user, String password) throws Exception
               {
                  return factory.createXATopicConnection(user, password);
               }
            });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void doCreateConnection(Class expectedInterface,
         ConnectionCreation creation) throws Exception
   {
      final ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
      final Map<String, Object> params = new HashMap<String, Object>();
      final long pingPeriod = 12987213;
      final long callTimeout = 27237;
      final String clientID = "kajsakjs";
      final int dupsOKBatchSize = 12344;
      final int defaultConsumerWindowSize = 1212;
      final int defaultConsumerMaxRate = 5656;
      final int defaultProducerWindowSize = 2323;
      final int defaultProducerMaxRate = 988;
      final boolean defaultBlockOnAcknowledge = true;
      final boolean defaultSendNonPersistentMessagesBlocking = true;
      final boolean defaultSendPersistentMessagesBlocking = true;
      
      JBossConnectionFactory factory = new JBossConnectionFactory(cf, params,
               pingPeriod, callTimeout, clientID, dupsOKBatchSize,
               defaultConsumerWindowSize, defaultConsumerMaxRate,
               defaultProducerWindowSize, defaultProducerMaxRate, defaultBlockOnAcknowledge,
               defaultSendNonPersistentMessagesBlocking, defaultSendPersistentMessagesBlocking);
      Object connection = creation.createConnection(factory);
      assertNotNull(connection);
      assertTrue(expectedInterface.isAssignableFrom(connection.getClass()));
   }

   private void doCreateConnectionWithCredentials(Class expectedInterface, String username, String password,
            ConnectionCreation creation) throws Exception
      {
      final ConnectorFactory cf = EasyMock.createMock(ConnectorFactory.class);
      final Map<String, Object> params = new HashMap<String, Object>();
      final long pingPeriod = 12987213;
      final long callTimeout = 27237;
         final String clientID = "kajsakjs";
         final int dupsOKBatchSize = 12344;        
         final int defaultConsumerWindowSize = 1212;
         final int defaultConsumerMaxRate = 5656;
         final int defaultProducerWindowSize = 2323;
         final int defaultProducerMaxRate = 988;
         final boolean defaultBlockOnAcknowledge = true;
         final boolean defaultSendNonPersistentMessagesBlocking = true;
         final boolean defaultSendPersistentMessagesBlocking = true;
         
         JBossConnectionFactory factory = new JBossConnectionFactory(cf, params,
                  pingPeriod, callTimeout,
                  clientID, dupsOKBatchSize,
                  defaultConsumerWindowSize, defaultConsumerMaxRate,
                  defaultProducerWindowSize, defaultProducerMaxRate, defaultBlockOnAcknowledge,
                  defaultSendNonPersistentMessagesBlocking, defaultSendPersistentMessagesBlocking);
         Object connection = creation.createConnection(factory, username, password);
         assertNotNull(connection);
         assertTrue(expectedInterface.isAssignableFrom(connection.getClass()));
      }


   // Inner classes -------------------------------------------------

   private class ConnectionCreation
   {
      Connection createConnection(JBossConnectionFactory factory)
            throws Exception
      {
         return null;
      }

      Connection createConnection(JBossConnectionFactory factory, String user,
            String password) throws Exception
      {
         return null;
      }
   }
}
