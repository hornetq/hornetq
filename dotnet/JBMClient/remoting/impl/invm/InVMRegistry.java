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
package org.jboss.messaging.core.remoting.impl.invm;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.logging.Logger;

/**
 * A InVMRegistry
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMRegistry
{
   private static final Logger log = Logger.getLogger(InVMRegistry.class);

   public static final InVMRegistry instance = new InVMRegistry();

   private ConcurrentMap<Integer, InVMAcceptor> acceptors = new ConcurrentHashMap<Integer, InVMAcceptor>();

   public void registerAcceptor(final int id, final InVMAcceptor acceptor)
   {     
      if (acceptors.putIfAbsent(id, acceptor) != null)
      {
         throw new IllegalArgumentException("Acceptor with id " + id + " already registered");
      }
   }

   public void unregisterAcceptor(final int id)
   {      
      if (acceptors.remove(id) == null)
      {
         throw new IllegalArgumentException("Acceptor with id " + id + " not registered");
      }
   }

   public InVMAcceptor getAcceptor(final int id)
   {
      return acceptors.get(id);
   }

   public void clear()
   {
      this.acceptors.clear();
   }

   public int size()
   {
      return this.acceptors.size();
   }
}
