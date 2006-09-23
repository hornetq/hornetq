/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import javax.management.ObjectName;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.QueuedExecutorPool;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * The base of a JBoss Messaging destination service. Both deployed or programatically created
 * destinations will eventually get one of these.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DestinationServiceSupport extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ObjectName serverPeerObjectName;
   
   protected boolean started = false;
   
   protected ManagedDestination destination;
   
   protected PostOffice postOffice;
      
   protected ServerPeer serverPeer;
   
   protected DestinationManager dm;
   
   protected SecurityManager sm;
   
   protected PersistenceManager pm;
   
   protected QueuedExecutorPool pool;
   
   protected MessageStore ms;
   
   protected TransactionRepository tr;
   
   protected IdManager idm;
   
   protected String nodeId;
   
   private boolean createdProgrammatically;
   
   // Constructors --------------------------------------------------
   
   public DestinationServiceSupport(boolean createdProgrammatically)
   {
      this.createdProgrammatically = createdProgrammatically;     
   }
   
   public DestinationServiceSupport()
   {
   }

   // ServiceMBeanSupport overrides -----------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {
         serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");
         
         dm = serverPeer.getDestinationManager();
         
         sm = serverPeer.getSecurityManager();
         
         pm = serverPeer.getPersistenceManagerInstance(); 
         
         pool = serverPeer.getQueuedExecutorPool();
         
         ms = serverPeer.getMessageStore();
         
         tr = serverPeer.getTxRepository();
         
         idm = serverPeer.getChannelIdManager();
         
         nodeId = serverPeer.getServerPeerID();
         
         String name = null;
                  
         if (serviceName != null)
         {
            name = serviceName.getKeyProperty("name");
         }
   
         if (name == null || name.length() == 0)
         {
            throw new IllegalStateException( "The " + (isQueue() ? "queue" : "topic") + " " +
                                             "name was not properly set in the service's" +
                                             "ObjectName");
         }
         
         destination.setName(name);                   
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }     
   }
   
   public synchronized void stopService() throws Exception
   {
      super.stopService();    
   }
   
   // JMX managed attributes ----------------------------------------

   public String getJNDIName()
   {
      return destination.getJndiName();
   }

   public void setJNDIName(String jndiName)
   {
      if (started)
      {
         log.warn("Cannot change the value of the JNDI name after initialization!");
         return;
      }

      destination.setJndiName(jndiName);
   }

   public void setServerPeer(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot change the value of associated " +
                  "server's ObjectName after initialization!");
         return;
      }

      serverPeerObjectName = on;
   }

   public ObjectName getServerPeer()
   {
      return serverPeerObjectName;
   }

   public void setSecurityConfig(Element securityConfig) throws Exception
   {
      try
      {
         if (started)
         {
            // push security update to the server
            sm.setSecurityConfig(isQueue(), destination.getName(), securityConfig);  
         }
   
         destination.setSecurityConfig(securityConfig);
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " setSecurityConfig");
      }
   }

   public Element getSecurityConfig()
   {
      return destination.getSecurityConfig();
   }

   public String getName()
   {
      return destination.getName();
   }


   /**
    * Get in-memory message limit
    * @return message limit
    */
   public int getFullSize()
   {
      return destination.getFullSize();
   }

   /**
    * Set in-memory message limit when destination is stopped.
    * @param fullSize the message limit
    */
   public void setFullSize(int fullSize)
   {
      if (started)
      {
         log.warn("FullSize can only be changed when destination is stopped");
         return;
      }      
      destination.setFullSize(fullSize);
   }

   /**
    * Get paging size
    * @return paging size
    */
   public int getPageSize()
   {
      return destination.getPageSize();
   }

   /**
    * Set paging size when destination is stopped.
    * @param pageSize the paging size
    */
   public void setPageSize(int pageSize)
   {
      if (started)
      {
         log.warn("PageSize can only be changed when destination is stopped");
         return;
      }
      destination.setPageSize(pageSize);
   }

   /**
    * Get write-cache size
    * @return cache size
    */
   public int getDownCacheSize()
   {
      return destination.getDownCacheSize();
   }

   /**
    * Set write-cache size when destination is stopped.
    * @param downCacheSize the cache size
    */
   public void setDownCacheSize(int downCacheSize)
   {
      if (started)
      {
         log.warn("DownCacheSize can only be changed when destination is stopped");
         return;
      }
      destination.setDownCacheSize(downCacheSize);
   }
   
   public boolean isClustered()
   {
      return destination.isClustered();
   }
   
   public void setClustered(boolean clustered)
   {
      if (started)
      {
         log.warn("Clustered can only be changed when destination is stopped");
         return;
      }
      destination.setClustered(clustered);
   }
   
   public boolean isCreatedProgrammatically()
   {
      return createdProgrammatically;
   }

   // JMX managed operations ----------------------------------------
   
   // TODO implement the following:

//   void removeAllMessages() throws Exception;
//
//   public MessageCounter[] getMessageCounter();
//
//   public MessageStatistics[] getMessageStatistics() throws Exception;
//
//   public String listMessageCounter();
//
//   public void resetMessageCounter();
//
//   public String listMessageCounterHistory();
//
//   public void resetMessageCounterHistory();
//
//   public void setMessageCounterHistoryDayLimit( int days );
//
//   public int getMessageCounterHistoryDayLimit();
//
//   public int getMaxDepth();
//
//   public void setMaxDepth(int depth);
//
//   public boolean getInMemory();
//
//   public void setInMemory(boolean mode);
//
//   public int getRedeliveryLimit();
//
//   public void setRedeliveryLimit(int limit);
//
//   public long getRedeliveryDelay();
//
//   public void setRedeliveryDelay(long rDelay);
//
//   public Class getReceiversImpl();
//
//   public void setReceiversImpl(Class receivers);
//
//   public int getRecoveryRetries();
//
//   public void setRecoveryRetries(int retries);

   // Public --------------------------------------------------------

   public String toString()
   {
      String nameFromJNDI = destination.getJndiName();
      int idx = -1;
      if (nameFromJNDI != null)
      {
         idx = nameFromJNDI.lastIndexOf('/');
      }
      if (idx != -1)
      {
         nameFromJNDI = nameFromJNDI.substring(idx + 1);
      }
      StringBuffer sb = new StringBuffer();
      if (isQueue())
      {
         sb.append("Queue");
      }
      else
      {
         sb.append("Topic");
      }
      sb.append('[');
      if (destination.getName().equals(nameFromJNDI))
      {
         sb.append(destination.getJndiName());
      }
      else
      {
         sb.append(destination.getJndiName()).append(", name=").append(destination.getName());
      }
      sb.append(']');
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract boolean isQueue();

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
