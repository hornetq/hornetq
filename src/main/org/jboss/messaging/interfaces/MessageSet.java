/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

/**
 * A message set.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageSet
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------
   
   /**
    * Add a message to the message set.
    * 
    * @param reference the message reference to add
    */
   void add(MessageReference reference);

   /**
    * Remove a message from the message set.
    * 
    * @param consumer the consumer used to accept the message
    * @return a message or null if there are no messages
    */
   MessageReference remove(Consumer consumer);

   /**
    * Lock the message set
    */
   void lock();
   
   /**
    * Unlock the message set
    */
   void unlock();
   
   /**
    * Set the consumer for out of band notifications
    * 
    * @param consumer the consumer
    */
   void setConsumer(Consumer consumer);
   
   // Inner Classes --------------------------------------------------
}
