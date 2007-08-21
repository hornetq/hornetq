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
package org.jboss.messaging.util;

import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;
import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;

/**
 * 
 * A LockMap.
 * 
 * This class effectively enables arbitrary objects to be locked
 * It does this by maintaining a mutex for each object in a map
 * When no more locks held, object is removed from the map
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1935 $</tt>
 *
 * $Id: LockMap.java 1935 2007-01-09 23:29:20Z clebert.suconic@jboss.com $
 */
public class LockMap
{ 
   protected Map map;
   
   private static class Entry
   {
      ReentrantLock lock = new ReentrantLock();
      int refCount;   
   }
   
   public static final LockMap instance = new LockMap();
   
   private LockMap()
   {
      this.map = new ConcurrentHashMap();
   }
   
//   public void obtainLock(Object obj)
//   {      
//      Entry entry = null;
//      synchronized (obj)
//      {
//         entry = (Entry)map.get(obj);
//         if (entry == null)
//         {
//            entry = new Entry();
//            map.put(obj, entry);
//         }        
//         entry.refCount++;
//      }
//      try
//      {
//         entry.lock.acquire();
//      }
//      catch (InterruptedException e)
//      {
//         throw new IllegalStateException("Thread interrupted while acquiring lock");
//      }
//   }
//   
//   public void releaseLock(Object obj)
//   {
//      synchronized (obj)
//      {
//         Entry entry = (Entry)map.get(obj);
//         if (entry == null)
//         {
//            throw new IllegalArgumentException("Cannot find mutex in map for " + obj);
//         }    
//         if (entry.refCount == 1)
//         {
//            map.remove(obj);
//         }
//         entry.refCount--;
//         entry.lock.release();         
//      }      
//   }
   
   public int getSize()
   {
      return map.size();
   }
}


