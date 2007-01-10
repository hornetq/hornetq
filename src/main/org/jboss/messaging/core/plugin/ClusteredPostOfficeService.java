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
package org.jboss.messaging.core.plugin;

import javax.management.ObjectName;
import javax.management.NotificationListener;
import javax.management.NotificationFilter;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.transaction.TransactionManager;
import org.jboss.jms.selector.SelectorFactory;
import org.jboss.jms.server.JMSConditionFactory;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.plugin.contract.ConditionFactory;
import org.jboss.messaging.core.plugin.contract.FailoverMapper;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultFailoverMapper;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.plugin.postoffice.cluster.Peer;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.JChannelFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.MultiplexerJChannelFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.jchannelfactory.XMLJChannelFactory;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.w3c.dom.Element;

import java.util.Set;
import java.util.Collections;

/**
 * A ClusteredPostOfficeService
 * 
 * MBean wrapper for a clustered post office
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ClusteredPostOfficeService extends JDBCServiceSupport implements Peer
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean started;

   // This group of properties is used on JGroups Channel configuration
   private Element syncChannelConfig;
   private Element asyncChannelConfig;
   private ObjectName channelFactoryName;
   private String syncChannelName;
   private String asyncChannelName;
   private String channelPartitionName;

   private ObjectName serverPeerObjectName;

   private String officeName;
   private long stateTimeout = 5000;
   private long castTimeout = 5000;
   private String groupName;
   private long statsSendPeriod = 1000;
   private String clusterRouterFactory;
   private String messagePullPolicy;

   private DefaultClusteredPostOffice postOffice;

   // Constructors --------------------------------------------------

   // ServerPlugin implementation -----------------------------------

   public MessagingComponent getInstance()
   {
      return postOffice;
   }

   // Peer implementation -------------------------------------------

   public Set getNodeIDView()
   {
      if (postOffice == null)
      {
         return Collections.EMPTY_SET;
      }

      return postOffice.getNodeIDView();
   }

   // NotificationBroadcaster implementation ------------------------

   public void addNotificationListener(NotificationListener listener,
                                       NotificationFilter filter,
                                       Object object) throws IllegalArgumentException
   {
      postOffice.addNotificationListener(listener, filter, object);
   }

   public void removeNotificationListener(NotificationListener listener)
      throws ListenerNotFoundException
   {
      postOffice.removeNotificationListener(listener);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return postOffice.getNotificationInfo();
   }


   // MBean attributes ----------------------------------------------

   public synchronized ObjectName getServerPeer()
   {
      return serverPeerObjectName;
   }

   public synchronized void setServerPeer(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.serverPeerObjectName = on;
   }

   public synchronized String getPostOfficeName()
   {
      return officeName;
   }

   public synchronized void setPostOfficeName(String name)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.officeName = name;
   }


   public ObjectName getChannelFactoryName()
   {
      return channelFactoryName;
   }

   public void setChannelFactoryName(ObjectName channelFactoryName)
   {
      this.channelFactoryName = channelFactoryName;
   }

   public String getSyncChannelName()
   {
      return syncChannelName;
   }

   public void setSyncChannelName(String syncChannelName)
   {
      this.syncChannelName = syncChannelName;
   }

   public String getAsyncChannelName()
   {
      return asyncChannelName;
   }

   public void setAsyncChannelName(String asyncChannelName)
   {
      this.asyncChannelName = asyncChannelName;
   }

   public String getChannelPartitionName()
   {
      return channelPartitionName;
   }

   public void setChannelPartitionName(String channelPartitionName)
   {
      this.channelPartitionName = channelPartitionName;
   }

   public void setSyncChannelConfig(Element config) throws Exception
   {
      syncChannelConfig = config;
   }

   public Element getSyncChannelConfig()
   {
      return syncChannelConfig;
   }

   public void setAsyncChannelConfig(Element config) throws Exception
   {
      asyncChannelConfig = config;
   }

   public Element getAsyncChannelConfig()
   {
      return asyncChannelConfig;
   }

   public void setStateTimeout(long timeout)
   {
      this.stateTimeout = timeout;
   }

   public long getStateTimeout()
   {
      return stateTimeout;
   }

   public void setCastTimeout(long timeout)
   {
      this.castTimeout = timeout;
   }

   public long getCastTimeout()
   {
      return castTimeout;
   }

   public void setGroupName(String groupName)
   {
      this.groupName = groupName;
   }

   public String getGroupName()
   {
      return groupName;
   }

   public void setStatsSendPeriod(long period)
   {
      this.statsSendPeriod = period;
   }

   public long getStatsSendPeriod()
   {
      return statsSendPeriod;
   }

   public String getClusterRouterFactory()
   {
      return clusterRouterFactory;
   }

   public String getMessagePullPolicy()
   {
      return messagePullPolicy;
   }

   public void setClusterRouterFactory(String clusterRouterFactory)
   {
      this.clusterRouterFactory = clusterRouterFactory;
   }

   public void setMessagePullPolicy(String messagePullPolicy)
   {
      this.messagePullPolicy = messagePullPolicy;
   }

   public String listBindings()
   {
      return postOffice.printBindingInformation();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // ServiceMBeanSupport overrides ---------------------------------

   protected synchronized void startService() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Service is already started");
      }

      super.startService();

      try
      {
         TransactionManager tm = getTransactionManagerReference();

         ServerPeer serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");
         MessageStore ms = serverPeer.getMessageStore();
         TransactionRepository tr = serverPeer.getTxRepository();
         PersistenceManager pm = serverPeer.getPersistenceManagerInstance();
         QueuedExecutorPool pool = serverPeer.getQueuedExecutorPool();
         int nodeId = serverPeer.getServerPeerID();

         Class clazz = Class.forName(messagePullPolicy);
         MessagePullPolicy pullPolicy = (MessagePullPolicy)clazz.newInstance();

         clazz = Class.forName(clusterRouterFactory);
         ClusterRouterFactory rf = (ClusterRouterFactory)clazz.newInstance();

         ConditionFactory cf = new JMSConditionFactory();
                  
         FilterFactory ff = new SelectorFactory();
         FailoverMapper mapper = new DefaultFailoverMapper();

         JChannelFactory jChannelFactory = null;

         if (channelFactoryName != null)
         {
            Object info = null;
            try
            {
               info = server.getMBeanInfo(channelFactoryName);
            }
            catch (Exception e)
            {
               // log.error("Error", e);
               // noop... means we couldn't find the channel hence we should use regular XMLChannelFactories
            }
            if (info!=null)
            {
               log.debug(this + " uses MultiplexerJChannelFactory");
               jChannelFactory =
                  new MultiplexerJChannelFactory(server, channelFactoryName, channelPartitionName,
                                                 syncChannelName, asyncChannelName);
            }
            else
            {
               log.debug(this + " uses XMLJChannelFactory");
               jChannelFactory = new XMLJChannelFactory(syncChannelConfig, asyncChannelConfig);
            }
         }
         else
         {
            log.debug(this + " uses XMLJChannelFactory");
            jChannelFactory = new XMLJChannelFactory(syncChannelConfig, asyncChannelConfig);
         }

         postOffice =  new DefaultClusteredPostOffice(ds, tm, sqlProperties,
                                                      createTablesOnStartup,
                                                      nodeId, officeName, ms,
                                                      pm, tr, ff, cf, pool,
                                                      groupName,
                                                      jChannelFactory,
                                                      stateTimeout, castTimeout,
                                                      pullPolicy, rf,
                                                      mapper,
                                                      statsSendPeriod);

         postOffice.start();

         started = true;
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   protected void stopService() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Service is not started");
      }

      super.stopService();

      try
      {
         postOffice.stop();

         postOffice = null;

         started = false;

         log.debug(this + " stopped");
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

