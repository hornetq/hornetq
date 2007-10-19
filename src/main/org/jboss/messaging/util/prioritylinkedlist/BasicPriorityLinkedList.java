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
package org.jboss.messaging.util.prioritylinkedlist;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.jboss.logging.Logger;

/**
 * A basic priority linked list
 * 
 * It implements this by maintaining an
 * individual LinkedList for each priority level.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: BasicPrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public class BasicPriorityLinkedList implements PriorityLinkedList
{      
   private static final Logger log = Logger.getLogger(BasicPriorityLinkedList.class);
   	
   protected LinkedList[] linkedLists;
   
   protected int priorities;
   
   protected int size;
   
   public void dump()
   {
   	log.debug("Dumping " + this);
   	log.debug("Size:" + size);
   	log.debug("===============");
   	
   	for (int i = 0; i < linkedLists.length; i++)
   	{
   		log.debug("Priority:" + i);
   		log.debug("----------------");
   		
   		Iterator iter = linkedLists[i].iterator();
   		
   		while (iter.hasNext())
   		{
   			log.debug("Ref: "+ iter.next());
   		}
   	}
   }
   
   public BasicPriorityLinkedList(int priorities)
   {
      this.priorities = priorities;
       
      initDeques();
   }
   
   public void addFirst(Object obj, int priority)
   {   
      linkedLists[priority].addFirst(obj);
      
      size++; 
   }
   
   public void addLast(Object obj, int priority)
   { 
      linkedLists[priority].addLast(obj);
      
      size++;
   }

   public Object removeFirst()
   {
      Object obj = null;
                  
      //Initially we are just using a simple prioritization algorithm:
      //Highest priority refs always get returned first.
      //This could cause starvation of lower priority refs.
      
      //TODO - A better prioritization algorithm
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         LinkedList ll = linkedLists[i];
         
         if (!ll.isEmpty())
         {
            obj = ll.removeFirst();
            break;
         }
                           
      }
      
      if (obj != null)
      {
         size--;
      }
      
      return obj;      
   }
   
   public Object removeLast()
   {
      Object obj = null;
      
      //Initially we are just using a simple prioritization algorithm:
      //Lowest priority refs always get returned first.
      
      //TODO - A better prioritization algorithm
            
      for (int i = 0; i < priorities; i++)
      {
         LinkedList ll = linkedLists[i];
         if (!ll.isEmpty())
         {
            obj = ll.removeLast();
         }
         if (obj != null)
         {
            break;
         }
      }
      
      if (obj != null)
      {
         size--;  
      }
           
      return obj;      
   }
   
   public Object peekFirst()
   {
      Object obj = null;
      
      //Initially we are just using a simple prioritization algorithm:
      //Highest priority refs always get returned first.
      //This could cause starvation of lower priority refs.
      
      //TODO - A better prioritization algorithm
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         LinkedList ll = linkedLists[i];
         if (!ll.isEmpty())
         {
            obj = ll.getFirst();
         }
         if (obj != null)
         {
            break;
         }
      }
      
      return obj;      
   }
   
   public List getAll()
   {
      List all = new ArrayList();
      for (int i = priorities - 1; i >= 0; i--)
      {
         LinkedList deque = linkedLists[i];
         all.addAll(deque);
      }
      return all;
   }
   
   public void clear()
   {
      initDeques();
   }
   
   public int size()
   {
      return size;
   }
   
   public boolean isEmpty()
   {
      return size == 0;
   }
   
   public ListIterator iterator()
   {
      return new PriorityLinkedListIterator(linkedLists);
   }
   
   protected void initDeques()
   {      
      linkedLists = new LinkedList[priorities];
      for (int i = 0; i < priorities; i++)
      {
         linkedLists[i] = new LinkedList();
      }
      
      size = 0;
   }
   
   
   class PriorityLinkedListIterator implements ListIterator
   { 
      private LinkedList[] lists;
      
      private int index;
      
      private ListIterator currentIter;
      
      PriorityLinkedListIterator(LinkedList[] lists)
      {
         this.lists = lists;
         
         index = lists.length - 1;
         
         currentIter = lists[index].listIterator();
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
            currentIter = lists[index].listIterator();
         }
         return currentIter.hasNext();      
      }
      
      public boolean hasPrevious()
      {
         throw new UnsupportedOperationException();
      }

      public Object next()
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

      public Object previous()
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
