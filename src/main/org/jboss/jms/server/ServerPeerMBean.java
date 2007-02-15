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
package org.jboss.jms.server;

import java.util.List;
import java.util.Set;

import javax.management.ObjectName;

import org.w3c.dom.Element;

/**
 * A ServerPeerMBean
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public interface ServerPeerMBean
{
   // JMX attributes
   
   int getServerPeerID();
   
   String getJMSVersion();

   int getJMSMajorVersion();

   int getJMSMinorVersion();

   String getJMSProviderName();

   String getProviderVersion();

   int getProviderMajorVersion();

   int getProviderMinorVersion();

   String getDefaultQueueJNDIContext();

   String getDefaultTopicJNDIContext();
   
   void setSecurityDomain(String securityDomain) throws Exception;

   String getSecurityDomain();

   void setDefaultSecurityConfig(Element conf) throws Exception;

   Element getDefaultSecurityConfig();
   
   ObjectName getPersistenceManager();

   void setPersistenceManager(ObjectName on);

   ObjectName getPostOffice();

   void setPostOffice(ObjectName on);

   ObjectName getJmsUserManager();

   void setJMSUserManager(ObjectName on);
   
   ObjectName getDefaultDLQ();

   void setDefaultDLQ(ObjectName on);
   
   ObjectName getDefaultExpiryQueue();

   void setDefaultExpiryQueue(ObjectName on);

   int getQueuedExecutorPoolSize();

   void setQueuedExecutorPoolSize(int poolSize);
   
   long getFailoverStartTimeout();
   
   void setFailoverStartTimeout(long timeout);
   
   long getFailoverCompleteTimeout();
   
   void setFailoverCompleteTimeout(long timeout);
   
   int getDefaultMaxDeliveryAttempts();

   void setDefaultMaxDeliveryAttempts(int attempts);   
   
   long getQueueStatsSamplePeriod();

   void setQueueStatsSamplePeriod(long newPeriod);
   
   long getDefaultRedeliveryDelay();
   
   void setDefaultRedeliveryDelay(long delay);
   
   int getDefaultMessageCounterHistoryDayLimit();
   
   void setDefaultMessageCounterHistoryDayLimit(int limit);
   
   List getMessageCounters() throws Exception;
   
   List getMessageStatistics() throws Exception;
   
   Set getDestinations() throws Exception;
   
   
   // JMX operations
   
   String createQueue(String name, String jndiName) throws Exception;

   String createQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize)
      throws Exception;

   boolean destroyQueue(String name) throws Exception;

   String createTopic(String name, String jndiName) throws Exception;

   String createTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize)
      throws Exception;

   boolean destroyTopic(String name) throws Exception;

   String listMessageCountersAsHTML() throws Exception;
   
   void resetAllMessageCounters();
   
   void resetAllMessageCounterHistories();
   
   List retrievePreparedTransactions();

   String showPreparedTransactionsAsHTML();
   
   void enableMessageCounters();
   
   void disableMessageCounters();
}
