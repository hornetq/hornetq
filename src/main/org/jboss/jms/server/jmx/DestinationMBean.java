/*
* JBoss, the OpenSource EJB server
*
* Distributable under LGPL license.
* See terms of license at gnu.org.
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
