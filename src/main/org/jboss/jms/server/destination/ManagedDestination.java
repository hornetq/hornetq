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

import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.messaging.core.plugin.contract.PostOffice;
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
   private static final int DEFAULT_FULL_SIZE = 75000;
   
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
   
   protected PostOffice postOffice;
   
   protected Queue dlq;
   
   protected Queue expiryQueue;
   
   public ManagedDestination()
   {      
   }
   
   /*
    * Constructor for temporary destinations
    */
   public ManagedDestination(String name, int fullSize, int pageSize, int downCacheSize)
   {
      this.name = name;
      this.fullSize = fullSize;
      this.pageSize = pageSize;
      this.downCacheSize = downCacheSize;
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

   public PostOffice getPostOffice()
   {
      return postOffice;
   }

   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public void setTemporary(boolean temporary)
   {
      this.temporary = temporary;
   }
   
   public Queue getDLQ()
   {
      return dlq;
   }
   
   public void setDLQ(Queue dlq)
   {
      this.dlq = dlq;
   }
   
   public Queue getExpiryQueue()
   {
      return expiryQueue;
   }
   
   public void setExpiryQueue(Queue expiryQueue)
   {
      this.expiryQueue = expiryQueue;
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
