/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * TODO: I will eventually implement this
 * @author Clebert Suconic
 */

public class IntervalSet<T extends Number> implements Set<T>
{
   int size = 0;
   @Override
   public int size()
   {
      return size;
   }

   @Override
   public boolean isEmpty()
   {
      return size() == 0;
   }

   @Override
   public boolean contains(Object o)
   {
      return false;
   }

   @Override
   public Iterator<T> iterator()
   {
      return null;
   }

   /**
    * Warning, this will create a big List containing every element on the list.
    * It could contain more elements that it would fit in memory
    * @return
    */
   @Override
   public Object[] toArray()
   {
      return new Object[0];
   }

   @Override
   public <T1> T1[] toArray(T1[] a)
   {
      return null;
   }

   @Override
   public boolean add(T t)
   {
      return false;
   }

   @Override
   public boolean remove(Object o)
   {
      return false;
   }

   @Override
   public boolean containsAll(Collection<?> c)
   {
      return false;
   }

   @Override
   public boolean addAll(Collection<? extends T> c)
   {
      return false;
   }

   @Override
   public boolean retainAll(Collection<?> c)
   {
      return false;
   }

   @Override
   public boolean removeAll(Collection<?> c)
   {
      return false;
   }

   @Override
   public void clear()
   {

   }
}
