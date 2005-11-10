/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * A Double-linked deque implementation that also allows fast removes from the middle.
 * 
 * This class is not currently synchronized.
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
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
      
      private DequeNode()
      {         
      }
      
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
