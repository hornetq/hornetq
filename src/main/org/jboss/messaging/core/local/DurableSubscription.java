/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import javax.jms.JMSException;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.PersistenceManager;

/**
 * 
 * Represents a durable topic subscription
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class DurableSubscription extends Subscription
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected String subName;
   
   // Constructors --------------------------------------------------

   public DurableSubscription(String subName, Topic topic, String selector,
                              MessageStore ms, PersistenceManager pm)
   {
      super(topic, selector, ms, pm);
      this.subName = subName;
   }
   

   // Channel implementation ----------------------------------------

   // Public --------------------------------------------------------
   
   public void closeConsumer(PersistenceManager pm) throws JMSException
   {
      //do nothing - this is durable
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

