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
package org.jboss.messaging.core.util;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * A ConcurrentReaderHashSet.
 * 
 * Offers same concurrency as ConcurrentHashMap but for a Set
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * ConcurrentReaderHashSet.java,v 1.1 2006/04/20 20:42:27 timfox Exp
 */
public class ConcurrentReaderHashSet extends AbstractSet
{
   private Map theMap;
   
   private static Object dummy = new Object();
   
   public ConcurrentReaderHashSet()
   {
      theMap = new ConcurrentReaderHashMap();
   }
   
   public int size()
   {
      return theMap.size();
   }
   
   public Iterator iterator()
   {
      return theMap.keySet().iterator();
   }
   
   public boolean isEmpty()
   {
      return theMap.isEmpty();
   }
   
   public boolean add(Object o)
   {
      return theMap.put(o, dummy) == dummy;
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

}
