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
package org.jboss.jms.server.jmx;

import javax.management.ObjectName;
import org.jboss.system.ServiceMBean;
import org.w3c.dom.Element;

/**
 * MBean interface for destinations
 *
 * @author <a href="pra@tim.se">Peter Antman</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public interface DestinationMBean extends ServiceMBean  
{
   ObjectName getServerPeer(); 
      
   void setServerPeer(ObjectName on); 

   void setJNDIName(String name);

   String getJNDIName();
   
   void setSecurityConfig(Element securityConf);
   
   
   /*
   
   TODO - We need to implement the following operations too
   in order to give equivalent functionality as JBossMQ

   void removeAllMessages() throws Exception; 
   
   public MessageCounter[] getMessageCounter();
   
   public MessageStatistics[] getMessageStatistics() throws Exception;

   public String listMessageCounter();
   
   public void resetMessageCounter();
   
   public String listMessageCounterHistory();
   
   public void resetMessageCounterHistory();

   public void setMessageCounterHistoryDayLimit( int days );

   public int getMessageCounterHistoryDayLimit();

   public int getMaxDepth();
   
   public void setMaxDepth(int depth);

   public boolean getInMemory();

   public void setInMemory(boolean mode);
   
   public int getRedeliveryLimit();

   public void setRedeliveryLimit(int limit);

   public long getRedeliveryDelay();

   public void setRedeliveryDelay(long rDelay);

   public Class getReceiversImpl();

   public void setReceiversImpl(Class receivers);

   public int getRecoveryRetries();

   public void setRecoveryRetries(int retries);
   
   */
}
