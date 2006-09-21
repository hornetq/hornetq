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
package org.jboss.messaging.core.plugin.postoffice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 
 * A DefaultBindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultBindings implements Bindings
{
   private List bindings;
   
   private int durableCount;
   
   public DefaultBindings()
   {
      bindings = new ArrayList();
   }

   public void addBinding(Binding binding)
   {
      if (bindings.contains(binding))
      {
         throw new IllegalArgumentException("Bindings already contains binding: " + binding);
      }
      bindings.add(binding);
      
      if (binding.getQueue().isRecoverable())
      {
         durableCount++;
      }
   }

   public Collection getAllBindings()
   {
      return bindings;
   }

   public boolean removeBinding(Binding binding)
   {
      boolean removed = bindings.remove(binding);
      
      if (removed && binding.getQueue().isRecoverable())
      {
         durableCount--;
      }
      
      return removed;
   }
   
   public int getDurableCount()
   {
      return durableCount;
   }
   
   public boolean isEmpty()
   {
      return bindings.isEmpty();
   }

}
