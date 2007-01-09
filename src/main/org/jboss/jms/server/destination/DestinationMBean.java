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

import javax.management.ObjectName;

import org.w3c.dom.Element;

/**
 * A DestinationMBean
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public interface DestinationMBean
{
   // JMX attributes
   
   String getName();

   String getJNDIName();

   void setJNDIName(String jndiName) throws Exception;

   ObjectName getServerPeer();
      
   void setServerPeer(ObjectName on);

   ObjectName getDLQ();
   
   void setDLQ(ObjectName on) throws Exception;
     
   ObjectName getExpiryQueue();   
   
   void setExpiryQueue(ObjectName on) throws Exception;
   
   long getRedeliveryDelay();
   
   void setRedeliveryDelay(long delay);
   
   int getMaxSize();
   
   void setMaxSize(int maxSize) throws Exception;
   
   Element getSecurityConfig();
   
   void setSecurityConfig(Element securityConfig) throws Exception;

   int getFullSize();

   void setFullSize(int fullSize);

   int getPageSize();

   void setPageSize(int pageSize);

   int getDownCacheSize();

   void setDownCacheSize(int downCacheSize);
   
   boolean isClustered();
   
   void setClustered(boolean clustered);
   
   boolean isCreatedProgrammatically();
   
   int getMessageCounterHistoryDayLimit();
   
   void setMessageCounterHistoryDayLimit(int limit) throws Exception;
   
   // JMX operations
   
   void removeAllMessages() throws Exception;
   
}
