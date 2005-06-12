/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.io.Serializable;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.local.LocalTopic;

/**
 * 
 * Holds state for a DurableSubscription
 *
 * @author <a href="tim.l.fox@gmail.com">Tim Fox/a>
 * @version $Revision$
 */
public class DurableSubscriptionHolder implements Serializable
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(DurableSubscriptionHolder.class);
   
   private static final long serialVersionUID = 6572385758103922254L;
   
   // Attributes ----------------------------------------------------
   
   private String name;
   private LocalQueue queue;
   private LocalTopic topic;
   private boolean hasConsumer;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public DurableSubscriptionHolder(String name, LocalTopic topic, LocalQueue queue)
   {
      this.name = name;
      this.queue = queue;
      this.topic = topic;
   }

   // Public --------------------------------------------------------
   

   public LocalQueue getQueue()
   {
      return queue;
   }
   
   public LocalTopic getTopic()
   {
      return topic;
   }
   
   public String getName()
   {
      return name;
   }
   
   public synchronized boolean hasConsumer()
   {
      return hasConsumer;
   }
   
   public synchronized void setHasConsumer(boolean hasConsumer)
   {
      if (log.isTraceEnabled()) log.trace("setHasConsumer:" + hasConsumer);
      this.hasConsumer = hasConsumer;
   }

   // Interface XXX implementation ----------------------------------
   
   // Object overrides -------------------------------------------
   
   public boolean equals(Object other)
   {
      if (this == other) return true;
      
      if (!(other instanceof DurableSubscriptionHolder)) return false;
      
      DurableSubscriptionHolder dOther = (DurableSubscriptionHolder)other;
      
      if (dOther.name == null || this.name == null) return false;
      
      return dOther.name.equals(this.name);
   }
   
   public int hashCode()
   {
      return name.hashCode();
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
     
}
