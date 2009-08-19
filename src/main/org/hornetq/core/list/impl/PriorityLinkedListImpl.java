/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
