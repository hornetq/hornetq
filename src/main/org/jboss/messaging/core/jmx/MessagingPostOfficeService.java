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
package org.jboss.messaging.core.jmx;

import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

import org.jboss.jms.server.JMSConditionFactory;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.selector.SelectorFactory;
import org.jboss.messaging.core.contract.ClusterNotifier;
import org.jboss.messaging.core.contract.ConditionFactory;
import org.jboss.messaging.core.contract.FilterFactory;
import org.jboss.messaging.core.contract.ChannelFactory;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.impl.IDManager;
import org.jboss.messaging.core.impl.jchannelfactory.MultiplexerChannelFactory;
import org.jboss.messaging.core.impl.jchannelfactory.XMLChannelFactory;
import org.jboss.messaging.core.impl.postoffice.MessagingPostOffice;
import org.jboss.messaging.core.impl.tx.TransactionRepository;
import org.jboss.messaging.util.ExceptionUtil;
import org.w3c.dom.Element;

/**
 * A MessagingPostOfficeService
 * 
 * MBean wrapper for a messaging post office
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2684 $</tt>
 *
 * $Id: ClusteredPostOfficeService.java 2684 2007-05-15 07:31:30Z timfox $
 *
 */
public class MessagingPostOfficeService extends JDBCServiceSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean started;

   // This group of properties is used on JGroups Channel configuration
   private Element controlChannelConfig;
   
   private Element dataChannelConfig;
   
   private ObjectName channelFactoryName;
   
   private String controlChannelName;
   
   private String dataChannelName;
   
   private String channelPartitionName;

   private ObjectName serverPeerObjectName;

   private String officeName;
   
   private long stateTimeout = 5000;
   
   private long castTimeout = 5000;
   
   private String groupName;
   
   private boolean clustered;

   private MessagingPostOffice postOffice;

   // Constructors --------------------------------------------------

   // ServerPlugin implementation -----------------------------------

   public MessagingComponent getInstance()
   {
      return postOffice;
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
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.channelFactoryName = channelFactoryName;
   }

   public String getControlChannelName()
   {
      return controlChannelName;
   }

   public void setControlChannelName(String controlChannelName)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.controlChannelName = controlChannelName;
   }

   public String getDataChannelName()
   {
      return dataChannelName;
   }

   public void setDataChannelName(String dataChannelName)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.dataChannelName = dataChannelName;
   }

   public String getChannelPartitionName()
   {
      return channelPartitionName;
   }

   public void setChannelPartitionName(String channelPartitionName)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.channelPartitionName = channelPartitionName;
   }

   public void setControlChannelConfig(Element config) throws Exception
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      controlChannelConfig = config;
   }

   public Element getControlChannelConfig()
   {
      return controlChannelConfig;
   }

   public void setDataChannelConfig(Element config) throws Exception
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      dataChannelConfig = config;
   }

   public Element getDataChannelConfig()
   {
      return dataChannelConfig;
   }

   public void setStateTimeout(long timeout)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
      this.stateTimeout = timeout;
   }

   public long getStateTimeout()
   {
      return stateTimeout;
   }

   public void setCastTimeout(long timeout)
   {
      if (started)
      {
         log.warn("Cannot set attribute when service is started");
         return;
      }
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
   
   public boolean isClustered()
   {
   	return clustered;
   }
   
   public void setClustered(boolean clustered)
   {
   	 if (started)
       {
          log.warn("Cannot set attribute when service is started");
          return;
       }
       this.clustered = clustered;
   }
   
   public String listBindings()
   {
      return postOffice.printBindingInformation();
   }
   
   public Set getNodeIDView()
   {
   	return postOffice.nodeIDView();
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
         
         PersistenceManager pm = serverPeer.getPersistenceManagerInstance();
         
         TransactionRepository tr = serverPeer.getTxRepository();
         
         IDManager idManager = serverPeer.getChannelIDManager();
         
         int nodeId = serverPeer.getServerPeerID();
         
         ClusterNotifier clusterNotifier = serverPeer.getClusterNotifier();

         ConditionFactory cf = new JMSConditionFactory();
                  
         FilterFactory ff = new SelectorFactory();
         
         ChannelFactory jChannelFactory = null;

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
               // noop... means we couldn't find the channel hence we should use regular
               // XMLChannelFactories
            }

            if (info != null)
            {
               log.debug(this + " uses MultiplexerJChannelFactory");

               jChannelFactory =
                  new MultiplexerChannelFactory(server, channelFactoryName, channelPartitionName,
                                                 controlChannelName, dataChannelName);
            }
            else
            {
               log.debug(this + " uses XMLJChannelFactory");
               jChannelFactory = new XMLChannelFactory(controlChannelConfig, dataChannelConfig);
            }
         }
         else
         {
            log.debug(this + " uses XMLJChannelFactory");
            jChannelFactory = new XMLChannelFactory(controlChannelConfig, dataChannelConfig);
         }

         if (clustered)
         {         
	         postOffice =  new MessagingPostOffice(ds, tm, sqlProperties,
	                                               createTablesOnStartup,
	                                               nodeId, officeName, ms,
	                                               pm,
	                                               tr, ff, cf, idManager,
	                                               clusterNotifier,
	                                               groupName,
	                                               jChannelFactory,
	                                               stateTimeout, castTimeout,
                                                  serverPeer.isSupportsFailover());
         }
         else
         {
         	postOffice =  new MessagingPostOffice(ds, tm, sqlProperties,
											                 createTablesOnStartup,
											                 nodeId, officeName, ms,
											                 pm,
											                 tr, ff, cf, idManager,
											                 clusterNotifier);
         }

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

