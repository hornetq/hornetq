/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.memory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.interfaces.MessageReference;
import org.jboss.messaging.interfaces.MessageSet;

import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;

/**
 * An in memory message set
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class MemoryMessageSet implements MessageSet
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   /** The messages */
   private Set messages;
   
   /** The lock */
   private ReentrantLock mutex = new ReentrantLock();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /**
    * Create a new MemoryMessageSet.
    *
    * @param comparator the comparator for the messages
    */
   public MemoryMessageSet(Comparator comparator)
   {
      messages = new TreeSet(comparator);
   }
   
   // Public --------------------------------------------------------

   // MessageSet implementation -------------------------------------

   public void add(MessageReference reference)
   {
      messages.add(reference);
   }

   public MessageReference remove(Consumer consumer)
   {
      // Do we have a message?
      if (messages.size() > 0)
      {
         for (Iterator iterator = messages.iterator(); iterator.hasNext();)
         {
            MessageReference message = (MessageReference) iterator.next();
            if (consumer.accepts(message, true))
            {
               iterator.remove();
               return message;
            }
         }
      }
      return null;
   }
   
   public void lock()
   {
      boolean interrupted = false;
      try
      {
         mutex.acquire();
      }
      catch (InterruptedException e)
      {
         interrupted = true;
      }
      if (interrupted)
         Thread.currentThread().interrupt();
   }

   public void unlock()
   {
      mutex.release();
   }
   
   public void setConsumer(Consumer consumer)
   {
      // There are no out of band notifications
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
