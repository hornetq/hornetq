/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.system.ServiceMBean;

/**
 * 
 * DestinationManager MBean Interface
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:ovidiu@jboss.org">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface DestinationManagerMBean extends ServiceMBean
{
  

   //int getClientCount();

   //Map getClients();

   /**
    * Get the value of PersistenceManager.
    * @return value of PersistenceManager.    */
   //ObjectName getPersistenceManager();

   /**
    * Set the value of PersistenceManager.
    * @param v Value to assign to PersistenceManager.    */
   //void setPersistenceManager(javax.management.ObjectName objectName);

   /**
    * Get the value of StateManager.
    * @return value of StateManager.    */
   //ObjectName getStateManager();

   /**
    * Set the value of StateManager.
    * @param v Value to assign to StateManager.    */
  // void setStateManager(ObjectName objectName);

   /**
    * Get the value of MessageCache.
    * @return value of MessageCache.    */
   //ObjectName getMessageCache();

   /**
    * Set the value of MessageCache.
    * @param v Value to assign to MessageCache.    */
   //void setMessageCache(ObjectName objectName);

   /**
    * Retrieve the temporary topic/queue max depth
    * @return the maximum depth
    */
   //int getTemporaryMaxDepth();

   /**
    * Set the temporary topic/queue max depth
    * @param depth the maximum depth
    */
   //void setTemporaryMaxDepth(int depth);

   /**
    * Retrieve the temporary topic/queue in memory mode
    * @return true for in memory
    */
  // boolean getTemporaryInMemory();

   /**
    * Set the temporary topic/queue in memory mode
    * @param mode true for in memory
    */
   //void setTemporaryInMemory(boolean mode);

   /**
    * Get the receivers implemenetation
    * @return the receivers implementation class
    */
   //Class getReceiversImpl();

   /**
    * Set the receivers implementation class
    * @param clazz the receivers implementation class
    */
   //void setReceiversImpl(Class clazz);

   /**
    * Returns the recovery retries
    */
   //public int getRecoveryRetries();

   /**
    * Sets the class implementating the receivers
    */
   //public void setRecoveryRetries(int retries);

   void createQueue(String name) throws Exception;

   void createTopic(String name) throws Exception;

   void createQueue(String name, String jndiLocation) throws Exception;

   void createTopic(String name, String jndiLocation) throws Exception;

   void destroyQueue(String name) throws Exception;

   void destroyTopic(String name) throws Exception;
   
   DestinationManager getDestinationManager();

   /**
    * Sets the destination message counter history day limit <0: unlimited, =0: disabled, > 0 maximum day count
    * @param days maximum day count
    */
   //void setMessageCounterHistoryDayLimit(int days);

   /**
    * Gets the destination message counter history day limit
    * @return Maximum day count
    */
   //int getMessageCounterHistoryDayLimit();

   /**
    * get message counter of all configured destinations
    */
   //MessageCounter[] getMessageCounter() throws Exception;

   /**
    * get message stats
    */
   //MessageStatistics[] getMessageStatistics() throws Exception;

   /**
    * List message counter of all configured destinations as HTML table
    */
   //listMessageCounter() throws java.lang.Exception;

   /**
    * Reset message counter of all configured destinations
    */
   //void resetMessageCounter();


   
}
