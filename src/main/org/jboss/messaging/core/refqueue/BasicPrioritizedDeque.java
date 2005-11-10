/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A basic non synchronized PrioritizedDeque implementation
 * It implements this by maintaining an individual DoubleLinkedDeque for each
 * priority level.
 * This should give significantly better performance than storing all objects in the same list
 * and applying some kind of ordering.
 * It also maintains a Map of references to Nodes to allow fast removes from the middle.
 * Currently it has a simple priority algorithm that always returns objects with the highest
 * priority first, if available. (Which can cause starvation)
 * 
 * This class is not synchronized.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 */
public class BasicPrioritizedDeque implements PrioritizedDeque
{
   protected DoubleLinkedDeque[] deques;
   
   protected int priorities;
   
   protected Map lookUp;
   
   public BasicPrioritizedDeque(int priorities)
   {
      this.priorities = priorities;
      init();
   }
   
   public void addFirst(Object obj, int priority)
   {
      Node node = deques[priority].addFirst(obj);  
      lookUp.put(obj, node);
   }

   public void addLast(Object obj, int priority)
   {
      Node node = deques[priority].addLast(obj);    
      lookUp.put(obj, node);
   }

   public boolean remove(Object obj)
   {      
      DoubleLinkedDeque.DequeNode node = (DoubleLinkedDeque.DequeNode)lookUp.remove(obj);
      if (node == null)
      {
         return false;
      }
      node.remove();
      return true;
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
         obj = deques[i].removeFirst();
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
         DoubleLinkedDeque queue = deques[i];
         all.addAll(queue.getAll());
      }
      return all;
   }
   
   public void clear()
   {
      init();
   }
   
   protected void init()
   {      
      deques = new DoubleLinkedDeque[priorities];
      for (int i = 0; i < priorities; i++)
      {
         deques[i] = new DoubleLinkedDeque();
      }
      lookUp = new HashMap();
   }
       
}
