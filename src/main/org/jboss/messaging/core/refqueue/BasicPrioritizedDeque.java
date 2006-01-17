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
 * A basic non synchronized PrioritizedDeque implementation. It implements this by maintaining an
 * individual DoubleLinkedDeque for each priority level. This should give significantly better
 * performance than storing all objects in the same list and applying some kind of ordering.<br>
 *
 * It also maintains a Map of references to Nodes to allow fast removes from the middle. Currently
 * it has a simple priority algorithm that always returns objects with the highest priority first,
 * if available (which can cause starvation).<br>
 * 
 * This class is not synchronized.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class BasicPrioritizedDeque implements PrioritizedDeque
{
   protected DoubleLinkedDeque[] deques;
   
   protected int priorities;
   
   public BasicPrioritizedDeque(int priorities)
   {
      this.priorities = priorities;
      init();
   }
   
   public boolean addFirst(Object obj, int priority)
   {
      return deques[priority].addFirst(obj);  
   }

   public boolean addLast(Object obj, int priority)
   {
      return deques[priority].addLast(obj);    
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
   
   public Object peekFirst()
   {
      Object obj = null;
      
      //Initially we are just using a simple prioritization algorithm:
      //Highest priority refs always get returned first.
      //This could cause starvation of lower priority refs.
      
      //TODO - A better prioritization algorithm
      
      for (int i = priorities - 1; i >= 0; i--)
      {
         obj = deques[i].peekFirst();
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
      //lookUp = new HashMap();
   }
       
}
