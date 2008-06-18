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

package org.jboss.messaging.core.list.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.jboss.messaging.core.list.PriorityLinkedList;

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
   private final List<LinkedList<T>> linkedLists;
   
   private final int priorities;
   
   private int size;
   
   public PriorityLinkedListImpl(final int priorities)
   {
      this.priorities = priorities;
       
      linkedLists = new ArrayList<LinkedList<T>>();
      
      for (int i = 0; i < priorities; i++)
      {
         linkedLists.add(new LinkedList<T>());
      }
   }
   
   public void addFirst(final T t, final int priority)
   {      
      linkedLists.get(priority).addFirst(t);
      
      size++; 
   }
   
   public void addLast(final T t, final int priority)
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
   	for (LinkedList<T> list: linkedLists)
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
      
      private ListIterator<T> currentIter;
      
      PriorityLinkedListIterator()
      {
         index = linkedLists.size() - 1;
         
         currentIter = linkedLists.get(index).listIterator();
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
