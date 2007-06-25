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
package org.jboss.jms.server.destination;

import java.util.Collection;
import java.util.Iterator;

import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.ServerPeer;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.Condition;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.w3c.dom.Element;

/**
 * A Destination
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public abstract class ManagedDestination implements MessagingComponent
{
   protected static final int ALL = 0;
   
   protected static final int DURABLE = 1;
   
   protected static final int NON_DURABLE = 2;
   
      
   private static final int DEFAULT_FULL_SIZE = 200000;
   
   private static final int DEFAULT_PAGE_SIZE = 2000;
   
   private static final int DEFAULT_DOWN_CACHE_SIZE = 2000;
      
   
   protected String name;
   
   protected String jndiName;
   
   protected boolean clustered;

   protected boolean temporary;
   
   // Default in memory message number limit
   protected int fullSize = DEFAULT_FULL_SIZE;

   // Default paging size
   protected int pageSize = DEFAULT_PAGE_SIZE;

   // Default down-cache size
   protected int downCacheSize = DEFAULT_DOWN_CACHE_SIZE;
   
   protected Element securityConfig;
   
   protected ServerPeer serverPeer;
   
   protected ManagedQueue dlq;
   
   protected ManagedQueue expiryQueue;
   
   protected long redeliveryDelay;
   
   protected int maxSize = -1;
   
   protected int messageCounterHistoryDayLimit = -1;
   
   protected int maxDeliveryAttempts = -1;
    
   public ManagedDestination()
   {      
   }
   
   /*
    * Constructor for temporary destinations
    */
   public ManagedDestination(String name, int fullSize, int pageSize, int downCacheSize, boolean clustered)
   {
      this.name = name;
      this.fullSize = fullSize;
      this.pageSize = pageSize;
      this.downCacheSize = downCacheSize;
      this.clustered = clustered;
   }

   public boolean isClustered()
   {
      return clustered;
   }

   public void setClustered(boolean clustered)
   {
      this.clustered = clustered;
   }

   public int getDownCacheSize()
   {
      return downCacheSize;
   }

   public void setDownCacheSize(int downCacheSize)
   {
      this.downCacheSize = downCacheSize;
   }

   public int getFullSize()
   {
      return fullSize;
   }

   public void setFullSize(int fullSize)
   {
      this.fullSize = fullSize;
   }

   public String getJndiName()
   {
      return jndiName;
   }

   public void setJndiName(String jndiName)
   {
      this.jndiName = jndiName;
   }

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public int getPageSize()
   {
      return pageSize;
   }

   public void setPageSize(int pageSize)
   {
      this.pageSize = pageSize;
   }

   public Element getSecurityConfig()
   {
      return securityConfig;
   }

   public void setSecurityConfig(Element securityConfig)
   {
      this.securityConfig = securityConfig;
   }
   
   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   public void setServerPeer(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public void setTemporary(boolean temporary)
   {
      this.temporary = temporary;
   }
   
   //Need to get lazily because of crappy dependencies
   public Queue getDLQ() throws Exception
   {
      Queue theQueue = null;
      
      if (dlq != null)
      {            
         Binding binding = serverPeer.getPostOfficeInstance().getBindingForQueueName(dlq.getName());
         
         if (binding == null)
         {
         	throw new IllegalStateException("Cannot find binding for queue " + dlq.getName());
         }
         
         Queue queue = binding.queue;
         
         if (queue.isActive())
         {
         	theQueue = queue;
         }
      }
      
      return theQueue;
   }
   
   public void setDLQ(ManagedQueue dlq)
   {
      this.dlq = dlq;
   }
   
   //Need to get lazily because of crappy dependencies
   public Queue getExpiryQueue() throws Exception
   {
      Queue theQueue = null;
      
      if (expiryQueue != null)
      {            
      	Binding binding = serverPeer.getPostOfficeInstance().getBindingForQueueName(expiryQueue.getName());
      	
         if (binding == null)
         {
         	throw new IllegalStateException("Cannot find binding for queue " + expiryQueue.getName());
         }
         
         Queue queue = binding.queue;
         
      	
      	if (queue.isActive())
      	{
      		theQueue = queue;
      	}
      }
      
      return theQueue;
   }
   
   public void setExpiryQueue(ManagedQueue expiryQueue)
   {
      this.expiryQueue = expiryQueue;
   }
   
   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }
   
   public void setRedeliveryDelay(long delay)
   {
      this.redeliveryDelay = delay;
   }
   
   public int getMaxSize()
   {
      return maxSize;
   }
   
   public void setMaxSize(int maxSize) throws Exception
   {
      Condition cond = new JMSCondition(isQueue(), name);
      
      PostOffice postOffice = serverPeer.getPostOfficeInstance();
      
      Collection subs = postOffice.getQueuesForCondition(cond, true);
      
      Iterator iter = subs.iterator();

      while (iter.hasNext())
      {
         Queue queue = (Queue)iter.next();
         
         queue.setMaxSize(maxSize);
      }
      
      this.maxSize = maxSize;
   }
   
   public int getMessageCounterHistoryDayLimit()
   {
      return this.messageCounterHistoryDayLimit;
   }
   
   public void setMessageCounterHistoryDayLimit(int limit) throws Exception
   {
      this.messageCounterHistoryDayLimit = limit;
   }
   
   public int getMaxDeliveryAttempts()
   {
      return this.maxDeliveryAttempts;
   }
   
   public void setMaxDeliveryAttempts(int maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }
     
   public abstract boolean isQueue();

   public void start() throws Exception
   {
      //NOOP
   }

   public void stop() throws Exception
   {   
      //NOOP
   }
}
