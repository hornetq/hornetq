/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import org.jboss.messaging.core.postoffice.BindingType;
import org.jboss.messaging.core.server.Bindable;
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
   private final BindingType type;
   
   private final SimpleString address;
   
   private final Bindable bindable;
   
   private final boolean exclusive;
   
   private volatile int weight = 1;
   
   private boolean hashAssigned;
   
   private int hash;
                
   public BindingImpl(final BindingType type, final SimpleString address, final Bindable bindable, final boolean exclusive)
   {
      this.type = type;
       
      this.address = address;
      
      this.bindable = bindable;
      
      this.exclusive = exclusive;
   }
   
   public BindingType getType()
   {
      return type;
   }
    
   public SimpleString getAddress()
   {
      return address;
   }
   
   public Bindable getBindable()
   {
      return bindable;
   }
   
   public boolean isExclusive()
   {
      return exclusive;
   }
   
   public void setWeight(final int weight)
   {
      this.weight = weight;      
   }
   
   public int getWeight()
   {
      return weight;
   }
         
   public boolean equals(Object other)
   {
      if (this == other)
      {
         return true;
      }
      
      Binding bother = (Binding)other;
      
      return (this.address.equals(bother.getAddress()) &&
              this.bindable.equals(bother.getBindable()));
   }
   
   public int hashCode()
   {
      if (!hashAssigned)
      {
         hash = 17;
         hash = 37 * hash + address.hashCode();
         hash = 37 * hash + bindable.hashCode();
                
         hashAssigned = true;
      }

      return hash;
   }
}
