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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 */
public interface MessageSet
{
   /**
    * Add a message to the message set.
    * 
    * @param message - the message to add.
    *
    * @return true if the message set did not already contained the specified message.
    */
   public boolean add(Message message);

   /**
    * Returns a random message from the set, without removing it.
    *
    * @return a random message. Returns null if there are no messages in the set. 
    */
   public Message get();

   /**
    * Remove a message from the message set.
    * 
    * @param message - the message to be removed.
    *
    * @return true if the set contained the specified element.
    */
   public boolean remove(Message message);

   /**
    * Lock the message set
    */
   public void lock();
   
   /**
    * Unlock the message set
    */
   public void unlock();
   
}