/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.jms.server.management;

import java.util.Set;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.server.impl.JMSFactoryType;

/**
 * A JMSServerControlUsingCoreTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerControlUsingJMSTest extends JMSServerControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   private static String[] toStringArray(final Object[] res)
   {
      String[] names = new String[res.length];
      for (int i = 0; i < res.length; i++)
      {
         names[i] = res[i].toString();
      }
      return names;
   }

   // Constructors --------------------------------------------------

   // JMSServerControlTest overrides --------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      HornetQConnectionFactory cf = (HornetQConnectionFactory) HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();

      connection = null;

      session = null;

      super.tearDown();
   }

   @Override
   protected JMSServerControl createManagementControl() throws Exception
   {
      HornetQQueue managementQueue = (HornetQQueue) HornetQJMSClient.createQueue("hornetq.management");
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_SERVER);

      return new JMSServerControl()
      {


         public boolean closeConnectionsForAddress(final String ipAddress) throws Exception
         {
            return (Boolean)proxy.invokeOperation("closeConnectionsForAddress", ipAddress);
         }

         public boolean createQueue(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name);
         }

         public boolean createQueue(String name, String jndiBindings, String selector) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBindings, selector);
         }

         public boolean createQueue(String name, String jndiBindings, String selector, boolean durable) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBindings, selector, durable);
         }
         
         public boolean createTopic(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createTopic", name);
         }

         public void destroyConnectionFactory(final String name) throws Exception
         {
            proxy.invokeOperation("destroyConnectionFactory", name);
         }

         public boolean destroyQueue(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyQueue", name);
         }

         public boolean destroyTopic(final String name) throws Exception
         {
            return (Boolean)proxy.invokeOperation("destroyTopic", name);
         }

         public String getVersion()
         {
            return (String)proxy.retrieveAttributeValue("version");
         }

         public boolean isStarted()
         {
            return (Boolean)proxy.retrieveAttributeValue("started");
         }

         public String[] getQueueNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("queueNames"));
         }

         public String[] getTopicNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("topicNames"));
         }

         public String[] getConnectionFactoryNames()
         {
            return JMSServerControlUsingJMSTest.toStringArray((Object[])proxy.retrieveAttributeValue("connectionFactoryNames"));
         }

         public String[] listConnectionIDs() throws Exception
         {
            return (String[])proxy.invokeOperation("listConnectionIDs");
         }
         
         public String listConnectionsAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listConnectionsAsJSON");
         }
         
         public String listConsumersAsJSON(String connectionID) throws Exception
         {
            return (String)proxy.invokeOperation("listConsumersAsJSON", connectionID);
         }

         public String[] listRemoteAddresses() throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses");
         }

         public String[] listRemoteAddresses(final String ipAddress) throws Exception
         {
            return (String[])proxy.invokeOperation("listRemoteAddresses", ipAddress);
         }

         public String[] listSessions(final String connectionID) throws Exception
         {
            return (String[])proxy.invokeOperation("listSessions", connectionID);
         }

         public void removeSecuritySettings(String addressMatch) throws Exception
         {
            proxy.invokeOperation("removeSecuritySettings", addressMatch);
         }
         
         @SuppressWarnings("unchecked")
         public Set<Role> getSecuritySettings(String addressMatch) throws Exception
         {
            return (Set<Role>)proxy.invokeOperation("getSecuritySettings", addressMatch);
         }
         
         public String getSecuritySettingsAsJSON(String addressMatch) throws Exception
         {
            return (String)proxy.invokeOperation("getSecuritySettingsAsJSON", addressMatch);
         }

         public boolean createQueue(String name, String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createQueue", name, jndiBinding);
         }

         public boolean createTopic(String name, String jndiBinding) throws Exception
         {
            return (Boolean)proxy.invokeOperation("createTopic", name, jndiBinding);
         }

         public String[] listTargetDestinations(String sessionID) throws Exception
         {
            return null;
         }

         public String getLastSentMessageID(String sessionID, String address) throws Exception
         {
            return null;
         }

         public String getSessionCreationTime(String sessionID) throws Exception
         {
            return (String)proxy.invokeOperation("getSessionCreationTime", sessionID);
         }

         public String listSessionsAsJSON(String connectionID) throws Exception
         {
            return (String)proxy.invokeOperation("listSessionsAsJSON", connectionID);
         }

         public String listPreparedTransactionDetailsAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listPreparedTransactionDetailsAsJSON");
         }

         public String listPreparedTransactionDetailsAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listPreparedTransactionDetailsAsHTML");
         }

         public void createConnectionFactory(String name,
                                             boolean ha,
                                             int cfType,
                                             String[] connectorNames,
                                             Object[] bindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, ha, cfType, connectorNames, bindings);
            
         }

         public void createConnectionFactory(String name, boolean ha, int cfType, String connectors, String jndiBindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, ha, cfType, connectors, jndiBindings);
         }

         public void createConnectionFactoryDiscovery(String name,
                                                      boolean ha,
                                                      int cfType,
                                                      String discoveryGroupName,
                                                      String bindings) throws Exception
         {
            proxy.invokeOperation("createConnectionFactory", name, ha, cfType, discoveryGroupName, bindings);
         }

         public void createConnectionFactoryDiscovery(String name,
                                                      boolean ha,
                                                      int cfType,
                                                      String discoveryGroupName,
                                                      Object[] bindings) throws Exception
         {
            // TODO Auto-generated method stub
            
         }

      };
   }
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
