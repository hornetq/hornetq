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

package org.jboss.messaging.jms.server.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSManagementService;
import org.jboss.messaging.util.JNDIUtil;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection
 * factories. Typically this would only be used in an app server env.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerManagerImpl implements JMSServerManager
{
   private static final Logger log = Logger
         .getLogger(JMSServerManagerImpl.class);

   /**
    * the initial context to bind to
    */
   private InitialContext initialContext;

   private final Map<String, List<String>> destinations = new HashMap<String, List<String>>();

   private final Map<String, JBossConnectionFactory> connectionFactories = new HashMap<String, JBossConnectionFactory>();

   private final Map<String, List<String>> connectionFactoryBindings = new HashMap<String, List<String>>();

   private final MessagingServerManagement messagingServerManagement;

   private JMSManagementService managementService;

   public JMSServerManagerImpl(final MessagingServerManagement serverManager, final JMSManagementService managementService)
   {
      messagingServerManagement = serverManager;
      this.managementService = managementService;
   }

   public void start() throws Exception
   {
      try
      {
         initialContext = new InitialContext();
      } catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
      managementService.registerJMSServer(this);
   }

   // JMSServerManager implementation -------------------------------

   public boolean isStarted()
   {
      return messagingServerManagement.isStarted();
   }

   public String getVersion()
   {
      return messagingServerManagement.getVersion();
   }

   public boolean createQueue(final String queueName, final String jndiBinding)
         throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);
      messagingServerManagement.addDestination(jBossQueue.getSimpleAddress());
      messagingServerManagement.createQueue(jBossQueue.getSimpleAddress(),
            jBossQueue.getSimpleAddress());
      boolean added = bindToJndi(jndiBinding, jBossQueue);
      if (added)
      {
         addToDestinationBindings(queueName, jndiBinding);
      }
      managementService.registerQueue(jBossQueue,
            messagingServerManagement.getQueue(jBossQueue.getSimpleAddress()),
            jndiBinding, this);
      return added;
   }

   public boolean createTopic(final String topicName, final String jndiBinding)
         throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      messagingServerManagement.addDestination(jBossTopic.getSimpleAddress());
      boolean added = bindToJndi(jndiBinding, jBossTopic);
      if (added)
      {
         addToDestinationBindings(topicName, jndiBinding);
      }
      managementService.registerTopic(jBossTopic,
            messagingServerManagement, jndiBinding);
      return added;
   }

   public boolean destroyQueue(final String name) throws Exception
   {
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      managementService.unregisterQueue(name);
      messagingServerManagement.removeDestination(JBossQueue
            .createAddressFromName(name));
      messagingServerManagement.destroyQueue(JBossQueue
            .createAddressFromName(name));

      return true;
   }

   public boolean destroyTopic(final String name) throws Exception
   {
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      managementService.unregisterTopic(name);
      messagingServerManagement.removeDestination(JBossTopic
            .createAddressFromName(name));

      return true;
   }

   public QueueSettings getSettings(final JBossDestination destination)
   {
      return messagingServerManagement.getQueueSettings(destination
            .getSimpleAddress());
   }

   public boolean createConnectionFactory(final String name,
         final String clientID, final int dupsOKBatchSize,
         final int consumerWindowSize, final int consumerMaxRate,
         final int producerWindowSize, final int producerMaxRate,
         final boolean blockOnAcknowledge,
         final boolean defaultSendNonPersistentMessagesBlocking,
         final boolean defaultSendPersistentMessagesBlocking,
         final String jndiBinding) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(clientID, dupsOKBatchSize,
               messagingServerManagement.getConfiguration().getLocation(),
               messagingServerManagement.getConfiguration()
                     .getConnectionParams(), consumerWindowSize,
               consumerMaxRate, producerWindowSize, producerMaxRate,
               blockOnAcknowledge, defaultSendNonPersistentMessagesBlocking,
               defaultSendPersistentMessagesBlocking);
         connectionFactories.put(name, cf);
      }
      if (!bindToJndi(jndiBinding, cf))
      {
         return false;
      }
      if (connectionFactoryBindings.get(name) == null)
      {
         connectionFactoryBindings.put(name, new ArrayList<String>());
      }
      connectionFactoryBindings.get(name).add(jndiBinding);

      List<String> bindings = new ArrayList<String>();
      bindings.add(jndiBinding);

      managementService.registerConnectionFactory(name, cf, bindings);
      return true;
   }

   public boolean createConnectionFactory(final String name,
         final String clientID, final int dupsOKBatchSize,
         final int consumerWindowSize, final int consumerMaxRate,
         final int producerWindowSize, final int producerMaxRate,
         final boolean blockOnAcknowledge,
         final boolean defaultSendNonPersistentMessagesBlocking,
         final boolean defaultSendPersistentMessagesBlocking,
         final List<String> jndiBindings) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         cf = new JBossConnectionFactory(clientID, dupsOKBatchSize,
               messagingServerManagement.getConfiguration().getLocation(),
               messagingServerManagement.getConfiguration()
                     .getConnectionParams(), consumerWindowSize,
               consumerMaxRate, producerWindowSize, producerMaxRate,
               blockOnAcknowledge, defaultSendNonPersistentMessagesBlocking,
               defaultSendPersistentMessagesBlocking);
      }
      for (String jndiBinding : jndiBindings)
      {
         bindToJndi(jndiBinding, cf);
         if (connectionFactoryBindings.get(name) == null)
         {
            connectionFactoryBindings.put(name, new ArrayList<String>());
         }
         connectionFactoryBindings.get(name).add(jndiBinding);
      }

      managementService.registerConnectionFactory(name, cf, jndiBindings);

      return true;
   }

   public boolean destroyConnectionFactory(final String name) throws Exception
   {
      List<String> jndiBindings = connectionFactoryBindings.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);

      managementService.unregisterConnectionFactory(name);

      return true;
   }

   public void removeAllMessages(final JBossDestination destination)
         throws Exception
   {
      messagingServerManagement.removeAllMessagesForAddress(destination
            .getSimpleAddress());
   }

   public boolean removeMessage(final long messageID,
         final JBossDestination destination) throws Exception
   {
      return messagingServerManagement.removeMessageFromAddress(messageID,
            destination.getSimpleAddress());
   }

   public int expireMessages(final Filter filter,
         final JBossDestination destination) throws Exception
   {
      return messagingServerManagement.expireMessages(filter, destination
            .getSimpleAddress());
   }

   public int sendMessagesToDLQ(final Filter filter,
         final JBossDestination destination) throws Exception
   {
      return messagingServerManagement.sendMessagesToDLQ(filter, destination
            .getSimpleAddress());
   }

   public int changeMessagesPriority(final Filter filter,
         final byte newPriority, final JBossDestination destination)
         throws Exception
   {
      return messagingServerManagement.changeMessagesPriority(filter,
            newPriority, destination.getSimpleAddress());
   }

   // Public --------------------------------------------------------

   public void setInitialContext(final InitialContext initialContext)
   {
      this.initialContext = initialContext;
   }

   // Private -------------------------------------------------------

   private boolean bindToJndi(final String jndiName, final Object objectToBind)
         throws NamingException
   {
      String parentContext;
      String jndiNameInContext;
      int sepIndex = jndiName.lastIndexOf('/');
      if (sepIndex == -1)
      {
         parentContext = "";
      } else
      {
         parentContext = jndiName.substring(0, sepIndex);
      }
      jndiNameInContext = jndiName.substring(sepIndex + 1);
      try
      {
         initialContext.lookup(jndiName);

         log.warn("Binding for " + jndiName + " already exists");
         return false;
      } catch (Throwable e)
      {
         // OK
      }

      Context c = JNDIUtil.createContext(initialContext, parentContext);

      c.rebind(jndiNameInContext, objectToBind);
      return true;
   }

   private void addToDestinationBindings(final String destination,
         final String jndiBinding)
   {
      if (destinations.get(destination) == null)
      {
         destinations.put(destination, new ArrayList<String>());
      }
      destinations.get(destination).add(jndiBinding);
   }
}
