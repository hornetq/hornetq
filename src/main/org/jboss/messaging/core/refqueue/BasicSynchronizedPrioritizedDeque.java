/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

import java.util.List;

/**
 * 
 * A BasicSynchronizedPrioritizedDeque applies very coarse synchronization to 
 * a PrioritizedDeque.
 * Not to be used for production use.
 * TODO Fine grained locking should be applied on ther DoubleLinkedDeque itself
 * to allow gets and puts to occur without contention when there are more than
 * x items in the deque
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class BasicSynchronizedPrioritizedDeque implements PrioritizedDeque
{
   
   protected PrioritizedDeque deque;
   
   public BasicSynchronizedPrioritizedDeque(PrioritizedDeque deque)
   {
      this.deque = deque;
   }

   public synchronized void addFirst(Object obj, int priority)
   {
      deque.addFirst(obj, priority);      
   }

   public synchronized void addLast(Object obj, int priority)
   {
      deque.addLast(obj, priority);
   }

   public synchronized boolean remove(Object obj)
   {
      return deque.remove(obj);
   }

   public synchronized Object removeFirst()
   {
      return deque.removeFirst();
   }

   public synchronized void clear()
   {
      deque.clear();
   }

   public synchronized List getAll()
   {
      return deque.getAll();
   }

}
