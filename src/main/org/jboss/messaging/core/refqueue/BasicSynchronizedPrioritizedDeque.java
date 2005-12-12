/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */

package org.jboss.messaging.core.refqueue;

import java.util.List;

/**
 * A BasicSynchronizedPrioritizedDeque applies very coarse synchronization to a PrioritizedDeque.
 * Not to be used for production use.
 *
 * TODO Fine grained locking should be applied on ther DoubleLinkedDeque itself to allow gets and
 *      puts to occur without contention when there are more than x items in the deque.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
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
