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
package org.jboss.messaging.core.list.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.list.PriorityHeadInsertableQueue;
import org.jboss.messaging.util.HeadInsertableQueue;
import org.jboss.messaging.util.HeadInsertableConcurrentQueue;

/**
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: BasicPrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public class PriorityHeadInsertableQueueImpl<T> implements PriorityHeadInsertableQueue<T>
{      	
   private final HeadInsertableQueue[] queues;
   
   private final int priorities;
   
   private AtomicInteger size = new AtomicInteger();
   
   public PriorityHeadInsertableQueueImpl(final int priorities)
   {
      this.priorities = priorities;
       
      queues = new HeadInsertableConcurrentQueue[priorities];
      
      for (int i = 0; i < priorities; i++)
      {
         queues[i] = new HeadInsertableConcurrentQueue<T>();
      }
   }
   
   public void offerFirst(final T t, final int priority)
   {      
      queues[priority].offerFirst(t);
      
      size.incrementAndGet();
   }
   
   public void offerLast(final T t, final int priority)
   { 
      queues[priority].offerLast(t);
      
      size.incrementAndGet();
   }

   public T poll()
   {         
      //Initially we are just using a simple prioritization algorithm:
      //Highest priority refs always get returned first.
      //This could cause starvation of lower priority refs.
      
      //TODO - A better prioritization algorithm
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         HeadInsertableQueue<T> ll = queues[i];
         
         if (ll.size() == 0)
         {
            continue;
         }
         
         T t = ll.poll();
         
         if (t != null)
         {
            size.decrementAndGet();
            
            return t;
         }                          
      }
      
      return null;    
   }
   
   public T peek()
   {
      for (int i = priorities - 1; i >= 0; i--)
      {
         HeadInsertableQueue<T> ll = queues[i];
         
         if (ll.size() == 0)
         {
            continue;
         }
         
         T t = ll.peek();
         
         if (t != null)
         {
            return t;
         }
      }
      
      return null;     
   }
   
   public List<T> getAll()
   {
      List<T> all = new ArrayList<T>();
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         HeadInsertableQueue<T> list = queues[i];
         
         //TODO improve performance
         for (T t: list)
         {
            all.add(t);
         }
      }
      
      return all;
   }
   
   public void clear()
   {
   	for (HeadInsertableQueue<T> list: queues)
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

   public Iterator<T> iterator()
   {
      return new PriorityHeadInsertableQueueImplIterator();
   }
      
   private class PriorityHeadInsertableQueueImplIterator implements ListIterator<T>
   { 
      private int index;
      
      private Iterator<T> currentIter;
      
      PriorityHeadInsertableQueueImplIterator()
      {
         index = queues.length - 1;
         
         currentIter = queues[index].iterator();
      }

      public void add(final Object obj)
      {
         throw new UnsupportedOperationException();
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
            
            currentIter = queues[index].iterator();
         }
         return currentIter.hasNext();      
      }
      
      public boolean hasPrevious()
      {
         throw new UnsupportedOperationException();
      }

      public T next()
      {
         if (!hasNext())
         {
            throw new NoSuchElementException();
         }
         return currentIter.next();
      }

      public int nextIndex()
      {
         throw new UnsupportedOperationException();
      }

      public T previous()
      {
         throw new UnsupportedOperationException();
      }

      public int previousIndex()
      {
         throw new UnsupportedOperationException();
      }

      public void remove()
      {
         currentIter.remove();      
         
         size.decrementAndGet();
      }

      public void set(final Object obj)
      {
         throw new UnsupportedOperationException();
      }
   }   
}
