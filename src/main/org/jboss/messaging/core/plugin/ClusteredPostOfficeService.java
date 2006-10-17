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
import javax.transaction.TransactionManager;

import org.jboss.jms.selector.SelectorFactory;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.FilterFactory;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.postoffice.cluster.ClusterRouterFactory;
import org.jboss.messaging.core.plugin.postoffice.cluster.DefaultClusteredPostOffice;
import org.jboss.messaging.core.plugin.postoffice.cluster.MessagePullPolicy;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.w3c.dom.Element;

/**
 * A ClusteredPostOfficeService
 * 
 * MBean wrapper for a clustered post office
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredPostOfficeService extends JDBCServiceSupport
{
   private DefaultClusteredPostOffice postOffice;
   
   private ObjectName serverPeerObjectName;
   
   private String officeName;
   
   private boolean started;
   
   private Element syncChannelConfig;
   
   private Element asyncChannelConfig;
   
   private long stateTimeout = 5000;
   
   private long castTimeout = 5000;
     
   private String groupName;
   
   private long statsSendPeriod = 1000;
   
   private String clusterRouterFactory;
   
   private String messagePullPolicy;
   
   // Constructors --------------------------------------------------------
   
   public ClusteredPostOfficeService()
   {      
   }
   
   // ServerPlugin implementation ------------------------------------------
   
   public MessagingComponent getInstance()
   {
      return postOffice;
   }
   
   // MBean attributes -----------------------------------------------------
   
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
         
         FilterFactory ff = new SelectorFactory();
            
         postOffice =  new DefaultClusteredPostOffice(ds, tm, sqlProperties, createTablesOnStartup,
                                               nodeId, officeName, ms,
                                               pm, tr, ff, pool, 
                                               groupName,
                                               syncChannelConfig, asyncChannelConfig,
                                               stateTimeout, castTimeout,
                                               pullPolicy, rf,
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
      
}

