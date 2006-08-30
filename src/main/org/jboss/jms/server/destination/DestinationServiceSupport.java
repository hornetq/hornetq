/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import javax.management.ObjectName;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.messaging.core.plugin.contract.Exchange;
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
   private static final int FULL_SIZE = 75000;
   private static final int PAGE_SIZE = 2000;
   private static final int DOWN_CACHE_SIZE = 2000;

   // Static --------------------------------------------------------

   /**
    * Destination implementations don't like empty string selectors, but this is how the jmx-console
    * sends arguments that are not filled out. This method pre-processes the selector String and
    * makes it a valid argument for destination methods.
    */
   static String trimSelector(String selector)
   {
      if (selector == null)
      {
         return null;
      }

      selector = selector.trim();

      if (selector.length() == 0)
      {
         return null;
      }

      return selector;
   }
   
   // Attributes ----------------------------------------------------

   protected ObjectName serverPeerObjectName;
   protected DestinationManager dm;
   protected SecurityManager sm;
   protected Exchange exchange;
   protected Element securityConfig;

   protected String name;
   protected String jndiName;

   protected boolean started = false;
   private boolean createdProgrammatically = false;
   
   // The following 3 attributes can only be changed when service is stopped.

   // Default in memory message number limit
   protected int fullSize = FULL_SIZE;

   // Default paging size
   protected int pageSize = PAGE_SIZE;

   // Default down-cache size
   protected int downCacheSize = DOWN_CACHE_SIZE;

   // Constructors --------------------------------------------------

   public DestinationServiceSupport(boolean createdProgrammatically)
   {
      this.createdProgrammatically = createdProgrammatically;
   }

   // JMX managed attributes ----------------------------------------

   public String getJNDIName()
   {
      return jndiName;
   }

   public void setJNDIName(String jndiName)
   {
      if (started)
      {
         log.warn("Cannot change the value of the JNDI name after initialization!");
         return;
      }

      this.jndiName = jndiName;
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
         // push security update to the server
         if (sm != null)
         {
            sm.setSecurityConfig(isQueue(), name, securityConfig);
         }
   
         this.securityConfig = securityConfig;
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " setSecurityConfig");
      }
   }

   public Element getSecurityConfig()
   {
      return securityConfig;
   }

   public String getName()
   {
      return name;
   }

   public boolean isCreatedProgrammatically()
   {
      return createdProgrammatically;
   }

   /**
    * Get in-memory message limit
    * @return message limit
    */
   public int getFullSize()
   {
      // XXX This value should be the same as getting from core destination
      return fullSize;
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
      this.fullSize = fullSize;
   }

   /**
    * Get paging size
    * @return paging size
    */
   public int getPageSize()
   {
      // XXX This value should be the same as getting from core destination
      return pageSize;
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
      this.pageSize = pageSize;
   }

   /**
    * Get write-cache size
    * @return cache size
    */
   public int getDownCacheSize()
   {
      // XXX This value should be the same as getting from core destination
      return downCacheSize;
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
      this.downCacheSize = downCacheSize;
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
      String nameFromJNDI = jndiName;
      int idx = -1;
      if (jndiName != null)
      {
         idx = jndiName.lastIndexOf('/');
      }
      if (idx != -1)
      {
         nameFromJNDI = jndiName.substring(idx + 1);
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
      if (name.equals(nameFromJNDI))
      {
         sb.append(jndiName);
      }
      else
      {
         sb.append(jndiName).append(", name=").append(name);
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
