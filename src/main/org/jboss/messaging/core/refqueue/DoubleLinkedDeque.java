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
import java.util.List;

/**
 * A double-linked deque implementation that also allows fast removes from the middle.<br>
 * 
 * This class is not currently synchronized.
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class DoubleLinkedDeque implements Deque
{
   protected DequeNode head;
   
   protected DequeNode tail;
         
   public Object removeFirst()
   {    
      Object ret = null;
      if (head != null)
      {
         ret = head.object;
         remove(head);
      }
      return ret;      
   }
            
   public Node addFirst(Object object)
   {         
      DequeNode node = new DequeNode(object, null, head);
      if (head != null)
      {
         head.prev = node;
      }
            
      head = node;
      if (tail == null)
      {
         tail = node;
      }
            
      return node;      
      
   }
   
   public Node addLast(Object object)
   {
      DequeNode node = new DequeNode(object, tail, null);
      if (tail != null)
      {
         tail.next = node;
      }
      tail = node;
      if (head == null)
      {
         head = node;
      }
      return node;      
   }
   
   public List getAll()
   {
      List all = new ArrayList();
      DequeNode n = head;
      while (n != null)
      {
         all.add(n.object);
         n = n.next;
      }
      return all;      
   }
   
   protected void remove(DequeNode node)
   {
      if (head == node)
      {
         if (node.next != null)
         {
            head = node.next;
         }
         else
         {
            head = null;
         }
      }
      
      if (tail == node)
      {
         if (node.prev != null)
         {
            tail = node.prev;
         }
         else
         {
            tail = null;
         }
      }
            
      if (node.prev != null)
      {
         node.prev.next = node.next;
      }
      
      if (node.next != null)
      {
         node.next.prev = node.prev;
      }
   }
   
  
   public class DequeNode implements Node
   {
      private Object object;
      
      private DequeNode prev;
      
      private DequeNode next;
      
      private DequeNode(Object object, DequeNode prev, DequeNode next)
      {
         this.object = object;
         this.prev = prev;
         this.next = next;
      }
      
      public void remove()
      {
         DoubleLinkedDeque.this.remove(this);
      }
      
      public Object getObject()
      {
         return object;
      }
      
   }
   
}
