/*
 * Copyright 2009 Red Hat, Inc.
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

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * A ConcurrentHashSet.
 * 
 * Offers same concurrency as ConcurrentHashMap but for a Set
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1935 $</tt>
 *
 * $Id: ConcurrentReaderHashSet.java 1935 2007-01-09 23:29:20Z clebert.suconic@jboss.com $
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> implements ConcurrentSet<E>
{
   private ConcurrentMap<E, Object> theMap;
   
   private static final Object dummy = new Object();
   
   public ConcurrentHashSet()
   {
      theMap = new ConcurrentHashMap<E, Object>();
   }
   
   public int size()
   {
      return theMap.size();
   }
   
   public Iterator<E> iterator()
   {
      return theMap.keySet().iterator();
   }
   
   public boolean isEmpty()
   {
      return theMap.isEmpty();
   }
   
   public boolean add(E o)
   {
      return theMap.put(o, dummy) == null;
   }
   
   public boolean contains(Object o)
   {
      return theMap.containsKey(o);
   }
   
   public void clear()
   {
      theMap.clear();
   }
   
   public boolean remove(Object o)
   {
      return theMap.remove(o) == dummy;
   }
   
   public boolean addIfAbsent(E o)
   {
   	Object obj = theMap.putIfAbsent(o, dummy);
   	
   	return obj == null;
   }

}
