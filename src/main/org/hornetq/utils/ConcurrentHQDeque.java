/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.utils;

import java.util.Iterator;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.concurrent.BlockingDeque;
import org.hornetq.utils.concurrent.HQIterator;
import org.hornetq.utils.concurrent.HornetQConcurrentLinkedQueue;
import org.hornetq.utils.concurrent.LinkedBlockingDeque;

/**
 * A ConcurrentHQDeque
 *
 * @author Tim Fox
 *
 *
 */
public class ConcurrentHQDeque<T> implements HQDeque<T>
{
   private static final Logger log = Logger.getLogger(ConcurrentHQDeque.class);

   private final HornetQConcurrentLinkedQueue<T> bodyQueue;
   
   private final BlockingDeque<T> headQueue;
   
   public ConcurrentHQDeque()
   {
      this.bodyQueue = new HornetQConcurrentLinkedQueue<T>();
      
      this.headQueue = new LinkedBlockingDeque<T>();
   }
   
   public synchronized void addFirst(T t)
   {
      headQueue.addFirst(t);
   }

   public void addLast(T t)
   {
      bodyQueue.add(t);
   }

   public void clear()
   {
      bodyQueue.clear();
      headQueue.clear();
   }

   public synchronized T getFirst()
   {     
      if (headQueue.isEmpty())
      {
         return bodyQueue.peek();
      }
      else
      {
         return headQueue.peek();
      }      
   }

   public boolean isEmpty()
   {
      return bodyQueue.isEmpty() && headQueue.isEmpty();
   }

   public HQIterator<T> iterator()
   {
      return new Iter();
   }

   public T removeFirst()
   {
      if (headQueue.isEmpty())
      {
         return bodyQueue.remove();
      }
      else
      {
         return headQueue.remove();
      }
   }

   private class Iter implements HQIterator<T>
   {
      private Iterator<T> headIter;
      
      private HQIterator<T> bodyIter;
      
      private boolean inHead;
      
      private Iter()
      {
         headIter = headQueue.iterator();
         
         bodyIter = bodyQueue.hqIterator();
      }

      public T next()
      {
         if (headIter.hasNext())
         {
            inHead = true;
            
            return headIter.next();
         }
         else
         {
            inHead = false;
            
            return bodyIter.next();
         }         
      }

      public void remove()
      {
         if (inHead)
         {
            headIter.remove();
         }
         else
         {
            bodyIter.remove();
         }
      }
      
   }
}
