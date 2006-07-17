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

import java.util.HashMap;
import java.util.Map;

import org.jboss.logging.Logger;

/**
 * A RotatingPool
 * 
 * This class makes sure requests on the same key always get the same value, and
 * values for a specific key are obtained from a fixed size array of instances in 
 * a rotating fashion
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public abstract class RotatingPool
{
   private static final Logger log = Logger.getLogger(RotatingPool.class);
   
   private int maxSize;
   
   private int pos;
   
   private Object[] entries;
   
   private Map keyMappings;
   
   public RotatingPool(int maxSize)
   { 
      this.maxSize = maxSize;
      
      this.entries = new Object[maxSize];
      
      this.keyMappings = new HashMap();
   }
   
   public synchronized Object get(Object key)
   {    
      //Does the mapping already exist?
      Object entry = keyMappings.get(key);
      
      if (entry == null)
      {
         //Create a new entry allocating from the rotating pool
         entry = entries[pos];
         
         if (entry == null)
         {
            entry = entries[pos] = createEntry();
         }
         
         keyMappings.put(key, entry);
         
         pos++;
         
         if (pos == maxSize)
         {
            pos = 0;
         }
      }
      
      return entry;
   }
   
   
   
   protected abstract Object createEntry();
   
}
