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
import java.util.LinkedList;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.concurrent.HQIterator;

/**
 * A NonConcurrentHQDeque
 *
 * @author Tim Fox
 *
 *
 */
public class NonConcurrentHQDeque<T> implements HQDeque<T>
{
   private static final Logger log = Logger.getLogger(ConcurrentHQDeque.class);

   private final LinkedList<T> queue;
   
   public NonConcurrentHQDeque()
   {
      this.queue = new LinkedList<T>();
   }
   
   public synchronized void addFirst(T t)
   {
      queue.addFirst(t);
   }

   public void addLast(T t)
   {
      queue.add(t);
   }

   public void clear()
   {
      queue.clear();
   }

   public synchronized T getFirst()
   {     
      return queue.getFirst();    
   }

   public boolean isEmpty()
   {
      return queue.isEmpty();
   }

   public HQIterator<T> iterator()
   {
      return new Iter();
   }

   public T removeFirst()
   {
      return queue.removeFirst();
   }

   private class Iter implements HQIterator<T>
   {
      private Iterator<T> iter;
      
      private Iter()
      {
         iter = queue.iterator();
      }

      public T next()
      {
         if (iter.hasNext())
         {
            return iter.next();
         }
         else
         {
            return null;
         }         
      }

      public void remove()
      {
         iter.remove();
      }
      
   }
}
