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
import java.util.LinkedList;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.ChannelSupport;

/**
 * A basic non synchronized PrioritizedDeque implementation.
 * 
 * It implements this by maintaining an
 * individual LinkedList for each priority level.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BasicPrioritizedDeque implements PrioritizedDeque
{     
   private static final Logger log = Logger.getLogger(BasicPrioritizedDeque.class);

   
   protected LinkedList[] linkedLists;
   
   protected int priorities;
   
   protected int size;
   
   public BasicPrioritizedDeque(int priorities)
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
   
   protected void initDeques()
   {      
      linkedLists = new LinkedList[priorities];
      for (int i = 0; i < priorities; i++)
      {
         linkedLists[i] = new LinkedList();
      }
      
      size = 0;
   }
   
}
