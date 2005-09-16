/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.local;

import org.jboss.messaging.core.TransactionalChannelSupport;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageStore;

import javax.transaction.TransactionManager;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class Topic extends TransactionalChannelSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public Topic(String name, MessageStore ms)
   {
      this(name, ms, null, null);
   }

   public Topic(String name, MessageStore ms, TransactionManager tm)
   {
      this(name, ms, null, tm);
   }

   public Topic(String name, MessageStore ms, PersistenceManager pm)
   {
      this(name, ms, pm, null);
   }

   public Topic(String name, MessageStore ms, PersistenceManager pm, TransactionManager tm)
   {
      super(name, ms, null, tm);
     
      /*
       * 
       * 
       FIXME - I don't get it - why would a topic ever be created with a persistence manager?
       If it has a pm then any non delivered persistent messages will get persisted
       Topics aren't supposed to persist unacked msgs. - Tim
        
       See: 
       
       JMS 1.1 Spec 6.12:
       
       Unacknowledged messages of a nondurable subscriber should be able to be
      recovered for the lifetime of that nondurable subscriber. When a nondurable
      subscriber terminates, messages waiting for it will likely be dropped whether
      or not they have been acknowledged.
      Only durable subscriptions are reliably able to recover unacknowledged
      messages.
      Sending a message to a topic with a delivery mode of PERSISTENT does not
      alter this model of recovery and redelivery. To ensure delivery, a TopicSubscriber
      should establish a durable subscription.

       */
      
      router = new PointToMultipointRouter();
   }

   // Channel implementation ----------------------------------------

   public boolean isStoringUndeliverableMessages()
   {
      return false;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreTopic[" + getChannelID() + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
