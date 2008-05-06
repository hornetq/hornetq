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
package org.jboss.messaging.core.postoffice.impl;

import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A BindingImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BindingImpl implements Binding
{ 
   private final SimpleString address;
   
   private final Queue queue;
   
   private boolean hashAssigned;
   
   private int hash;
      
   public BindingImpl(final SimpleString address, final Queue queue)
   {
      this.address = address;
      
      this.queue = queue;
   }
   
   public SimpleString getAddress()
   {
      return address;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public boolean equals(Object other)
   {
      if (this == other)
      {
         return true;
      }
      Binding bother = (Binding)other;
      
      return (this.address.equals(bother.getAddress()) &&
              this.queue.equals(bother.getQueue()));
   }
   
   public int hashCode()
   {
      if (!hashAssigned)
      {
         hash = 17;
         hash = 37 * hash + address.hashCode();
         hash = 37 * hash + queue.hashCode();
                
         hashAssigned = true;
      }

      return hash;
   }
}
