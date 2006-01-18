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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Single-linked double ended queue.
 * 
 * Uses fine grained synchronization, so that if there are one or more objects in the deque
 * adds and removes can take place concurrently without lock contention.
 * 
 * Note! The user MUST ensure that adds cannot take place concurrently.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * SingleLinkedDeque.java,v 1.1 2006/01/18 12:52:38 timfox Exp
 */
public class SingleLinkedDeque implements Deque
{
   protected DequeNode head;
   
   protected DequeNode tail;
   
   protected int addCount;
   
   protected int removeCount;
   
   public SingleLinkedDeque()
   {
      head = tail = new DequeNode(null, null);      
   }
         
   public Object removeFirst()
   {          
      synchronized (head)
      {
         Object ret = null;
         
         if (head.next != null)
         {
            DequeNode first = (DequeNode)head.next;
                        
            ret = first.object;
            
            head.next = first.next;
            
            if (head.next == null)
            {
               tail = head;
            }
            
            //Incrementing int is atomic
            removeCount++;
         }                  
                 
         return ret;
      }          
   }
   
   public Object peekFirst()
   {    
      synchronized (head)
      {
         Object ret = null;
         
         if (head.next != null)
         {
            DequeNode first = (DequeNode)head.next;
            
            ret = first.object;            
         }
         return ret;
      }      
   }
            

   public void addFirst(Object object)
   {         
      synchronized (head)
      {
         DequeNode first = (DequeNode)head.next;
         
         DequeNode node = new DequeNode(object, first);
         
         head.next = node;
         
         if (first == null)
         {
            tail = node;
         }
         
         //Incrementing int is atomic
         addCount++;
         
         if (addCount == Integer.MAX_VALUE)
         {
            adjustCounts();
         }
      }               
   }
   
   public void addLast(Object object)
   {             
      synchronized (tail)
      {
         DequeNode node = new DequeNode(object, null);
         
         tail.next = node;
         
         tail = node;
         
         addCount++;
         
         if (addCount == Integer.MAX_VALUE)
         {
            adjustCounts();
         }
      }       
   }
   
   class DequeIterator implements Iterator
   {
      DequeNode next;
      
      DequeIterator()
      {
         next = head.next;
      }

      public boolean hasNext()
      {
         return next != null;
      }

      public Object next()
      {
         if (next == null)
         {
            throw new NoSuchElementException("No more elements in DequeIterator");
         }
         Object ret = next.object;
         next = next.next;
         return ret;
      }

      public void remove()
      {
         throw new UnsupportedOperationException();
      }
      
   }
   
   public Iterator iterator()
   {
      return new DequeIterator();
   }
   
   public int size()
   {
      //Keeping a direct count of the number of objects in the deque
      //would require synchronizing a value between the add and remove methods
      //thus causing thread contention for locks.
      //To avoid this we keep separate add and remove counts and only zero them
      //every 100000 objects or so
      
      return addCount - removeCount;
   }
   
   private void adjustCounts()
   {      
      synchronized (head)
      {
         addCount = addCount - removeCount;
         
         removeCount = 0;
      }
   }
   
   public List getAll()
   {
      //TODO Currently we prevent getAll() executing concurrently with any adds or removes
      //getAll() is primarily used for the JMS browse functionality which doesn't require that the set
      //returned exactly reflects the contents of the queue.
      //Therefore we could relax the exclusive locking which allow better concurrency of browses(),
      //adds and removes
      synchronized (head)
      {
         List all = new ArrayList();
         
         DequeNode n = head.next;
         
         while (n != null)
         {         
            all.add(n.object);
            
            n = n.next;
         }
         
         return all;             
      } 
   }
     
   private class DequeNode
   {
      private Object object;
      
      private DequeNode next;
      
      private DequeNode(Object object, DequeNode next)
      {
         this.object = object;
         
         this.next = next;
      }
      
      public Object getObject()
      {
         return object;
      }
      
   }
   
}

