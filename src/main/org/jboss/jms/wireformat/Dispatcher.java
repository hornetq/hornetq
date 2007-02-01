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
package org.jboss.jms.wireformat;

import java.util.Map;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A NewJMSDispatcher
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class Dispatcher
{
   public static Dispatcher instance = new Dispatcher();
   
   private Map targets;
   
   private Dispatcher()
   {
      targets = new ConcurrentReaderHashMap();
   }
   
   public Object getTarget(int id)
   {
      return getTarget(new Integer(id));
   }
   
   public Object getTarget(Integer id)
   {
      return targets.get(id);
   }
   
   public void registerTarget(Integer id, Object obj)
   {
      targets.put(id, obj);
   }
   
   public void registerTarget(int id, Object obj)
   {
      registerTarget(new Integer(id), obj);
   }
   
   public boolean unregisterTarget(Integer id)
   {
      return targets.remove(id) != null;
   }
   
   public boolean unregisterTarget(int id)
   {
      return unregisterTarget(new Integer(id));
   }
   
}
