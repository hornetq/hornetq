/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

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
    * Add a routable to the routable set.
    * 
    * @param routable - the routable to add.
    *
    * @return true if the routable set did not already contained the specified routable.
    */
   public boolean add(Routable routable);

   /**
    * Returns a random message from the set, without removing it.
    *
    * @return a random message. Returns null if there are no messages in the set. 
    */
   public Routable get();

   /**
    * Remove a routable from the routable set.
    * 
    * @param routable - the routable to be removed.
    *
    * @return true if the set contained the specified element.
    */
   public boolean remove(Routable routable);

   /**
    * Lock the message set
    */
   public void lock();
   
   /**
    * Unlock the message set
    */
   public void unlock();
   
}