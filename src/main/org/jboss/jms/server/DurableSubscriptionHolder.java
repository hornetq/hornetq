/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.io.Serializable;

import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.local.LocalTopic;

/**
 * 
 * Holds state for a DurableSubscription
 *
 * @author <a href="tim.l.fox@gmail.com">Tim Fox/a>
 * @version $Revision$
 *
 * $Id$
 */
public class DurableSubscriptionHolder implements Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 6572385758103922254L;
   
   // Attributes ----------------------------------------------------
   
   private String name;
   private LocalQueue queue;
   private LocalTopic topic;
   private String selector;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public DurableSubscriptionHolder(String name, LocalTopic topic, LocalQueue queue, String selector)
   {
      this.name = name;
      this.queue = queue;
      this.topic = topic;
      this.selector = selector;
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
   
   public String getSelector()
   {
      return selector;
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
