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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.PriorityLinkedList;

/**
 * A priority linked list implementation
 * 
 * It implements this by maintaining an individual LinkedList for each priority level.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: BasicPrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public class PriorityLinkedListImpl<T> implements PriorityLinkedList<T>
{      
   private static final Logger log = Logger.getLogger(PriorityLinkedListImpl.class);
   	
   private List<LinkedList<T>> linkedLists;
   
   private int priorities;
   
   private int size;
   
   public PriorityLinkedListImpl(int priorities)
   {
      this.priorities = priorities;
       
      initLists();
   }
   
   public void addFirst(T t, int priority)
   {      
      linkedLists.get(priority).addFirst(t);
      
      size++; 
   }
   
   public void addLast(T t, int priority)
   { 
      linkedLists.get(priority).addLast(t);
      
      size++;
   }

   public T removeFirst()
   {
      T t = null;
                  
      //Initially we are just using a simple prioritization algorithm:
      //Highest priority refs always get returned first.
      //This could cause starvation of lower priority refs.
      
      //TODO - A better prioritization algorithm
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         LinkedList<T> ll = linkedLists.get(i);
         
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
   
   public T removeLast()
   {
      T t = null;
           
      for (int i = 0; i < priorities; i++)
      {
         LinkedList<T> ll = linkedLists.get(i);
         if (!ll.isEmpty())
         {
            t = ll.removeLast();
         }
         if (t != null)
         {
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
         LinkedList<T> ll = linkedLists.get(i);
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
         LinkedList<T> list = linkedLists.get(i);
         all.addAll(list);
      }
      
      return all;
   }
   
   public void clear()
   {
      initLists();
   }
   
   public int size()
   {
      return size;
   }
   
   public boolean isEmpty()
   {
      return size == 0;
   }
   
   public ListIterator<T> iterator()
   {
      return new PriorityLinkedListIterator();
   }
   
   public void dump()
   {
      log.debug("Dumping " + this);
      log.debug("Size:" + size);
      log.debug("===============");
      
      for (int i = 0; i < linkedLists.size(); i++)
      {
         log.debug("Priority:" + i);
         log.debug("----------------");
         
         Iterator<T> iter = linkedLists.get(i).iterator();
         
         while (iter.hasNext())
         {
            log.debug("Ref: "+ iter.next());
         }
      }
   }
      
   private void initLists()
   {      
      linkedLists = new ArrayList<LinkedList<T>>();
      
      for (int i = 0; i < priorities; i++)
      {
         linkedLists.add(new LinkedList<T>());
      }
      
      size = 0;
   }
      
   private class PriorityLinkedListIterator implements ListIterator<T>
   { 
      private int index;
      
      private ListIterator<T> currentIter;
      
      PriorityLinkedListIterator()
      {
         index = linkedLists.size() - 1;
         
         currentIter = linkedLists.get(index).listIterator();
      }

      public void add(Object arg0)
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
            currentIter = linkedLists.get(index).listIterator();
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
         
         size--;
      }

      public void set(Object obj)
      {
         throw new UnsupportedOperationException();
      }
   }   
}
