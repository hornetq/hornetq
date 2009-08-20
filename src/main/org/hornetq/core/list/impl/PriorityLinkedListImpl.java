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

package org.hornetq.core.list.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hornetq.core.list.PriorityLinkedList;
import org.hornetq.utils.concurrent.Deque;
import org.hornetq.utils.concurrent.LinkedBlockingDeque;

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
   private final List<Deque<T>> levels;

   private final int priorities;

   private int size;

   public PriorityLinkedListImpl(final int priorities)
   {
      this.priorities = priorities;

      levels = new ArrayList<Deque<T>>();

      for (int i = 0; i < priorities; i++)
      {
         levels.add(new LinkedBlockingDeque<T>());
      }
   }

   public void addFirst(final T t, final int priority)
   {
      levels.get(priority).addFirst(t);

      size++;
   }

   public void addLast(final T t, final int priority)
   {
      levels.get(priority).addLast(t);

      size++;
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
         Deque<T> ll = levels.get(i);

         if (!ll.isEmpty())
         {
            t = ll.removeFirst();
            break;
         }
      }

      if (t != null)
      {
         size--;
      }

      return t;
   }

   public T peekFirst()
   {
      T t = null;

      for (int i = priorities - 1; i >= 0; i--)
      {
         Deque<T> ll = levels.get(i);
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

   public List<T> getAll()
   {
      List<T> all = new ArrayList<T>();

      for (int i = priorities - 1; i >= 0; i--)
      {
         Deque<T> list = levels.get(i);
         all.addAll(list);
      }

      return all;
   }

   public void clear()
   {
      for (Deque<T> list : levels)
      {
         list.clear();
      }

      size = 0;
   }

   public int size()
   {
      return size;
   }

   public boolean isEmpty()
   {
      return size == 0;
   }

   public Iterator<T> iterator()
   {
      return new PriorityLinkedListIterator();
   }

   private class PriorityLinkedListIterator implements Iterator<T>
   {
      private int index;

      private Iterator<T> currentIter;

      PriorityLinkedListIterator()
      {
         index = levels.size() - 1;

         currentIter = levels.get(index).iterator();
      }

      public boolean hasNext()
      {
         if (currentIter.hasNext())
         {
            return true;
         }

         while (index >= 0)
         {
            if (index == 0 || currentIter.hasNext())
            {
               break;
            }

            index--;

            currentIter = levels.get(index).iterator();
         }
         return currentIter.hasNext();
      }

      public T next()
      {
         if (!hasNext())
         {
            throw new NoSuchElementException();
         }
         return currentIter.next();
      }

      public void remove()
      {
         currentIter.remove();

         size--;
      }
   }
}
