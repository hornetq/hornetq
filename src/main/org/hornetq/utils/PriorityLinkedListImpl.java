/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.utils;

import java.lang.reflect.Array;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.concurrent.HQIterator;

/**
 * A priority linked list implementation
 * 
 * It implements this by maintaining an individual LinkedBlockingDeque for each priority level.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com>Jeff Mesnil</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: BasicPrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public class PriorityLinkedListImpl<T> implements PriorityLinkedList<T>
{
   private static final Logger log = Logger.getLogger(PriorityLinkedListImpl.class);

   private HQDeque<T>[] levels;

   private final int priorities;

   private final AtomicInteger size = new AtomicInteger(0);

   public PriorityLinkedListImpl(final boolean concurrent, final int priorities)
   {
      this.priorities = priorities;

      levels = (HQDeque<T>[])Array.newInstance(HQDeque.class, priorities);

      for (int i = 0; i < priorities; i++)
      {
         if (concurrent)
         {
            levels[i] = new ConcurrentHQDeque<T>();
         }
         else
         {
            levels[i] = new NonConcurrentHQDeque<T>();
         }
      }
   }

   public int addFirst(final T t, final int priority)
   {
      levels[priority].addFirst(t);
      
      return size.incrementAndGet();
   }

   public int addLast(final T t, final int priority)
   {
      levels[priority].addLast(t);
      
      return size.incrementAndGet();
   }

   public T removeFirst()
   {
      T t = null;

      // Initially we are just using a simple prioritization algorithm:
      // Highest priority refs always get returned first.
      // This could cause starvation of lower priority refs.

      // TODO - A better prioritization algorithm

      for (int i = priorities - 1; i >= 0; i--)
      {
         HQDeque<T> ll = levels[i];

         if (!ll.isEmpty())
         {
            t = ll.removeFirst();
            break;
         }
      }

      if (t != null)
      {
         size.decrementAndGet();
      }

      return t;
   }

   public T peekFirst()
   {
      T t = null;

      for (int i = priorities - 1; i >= 0; i--)
      {
         HQDeque<T> ll = levels[i];
         if (!ll.isEmpty())
         {
            t = ll.getFirst();
         }
         if (t != null)
         {
            break;
         }
      }

      return t;
   }

   public void clear()
   {
      for (HQDeque<T> list : levels)
      {
         list.clear();
      }

      size.set(0);
   }

   public int size()
   {
      return size.get();
   }

   public boolean isEmpty()
   {
      return size.get() == 0;
   }

   public HQIterator<T> iterator()
   {
      return new PriorityLinkedListIterator();
   }

   private class PriorityLinkedListIterator implements HQIterator<T>
   {
      private int index;

      private HQIterator<T>[] cachedIters = new HQIterator[levels.length]; 

      PriorityLinkedListIterator()
      {
         index = levels.length - 1;
      }

      public T next()
      {
         while (index >= 0)
         {
            HQIterator<T> iter = cachedIters[index];
            
            if (iter == null)
            {
               iter = cachedIters[index] = levels[index].iterator();
            }
            
            T t = iter.next();
            
            if (t != null)
            {
               return t;
            }
            
            index--;
            
            if (index < 0)
            {
               index = levels.length - 1;
               
               break;
            }
         }
         
         return null;
      }

      public void remove()
      {
         HQIterator<T> iter = cachedIters[index];
         
         if (iter == null)
         {
            throw new NoSuchElementException();
         }
         
         iter.remove();

         size.decrementAndGet();
      }
   }
}
