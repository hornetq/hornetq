/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import org.jboss.messaging.core.postoffice.DivertBinding;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.Divert;
import org.jboss.messaging.util.SimpleString;

/**
 * A DivertBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 9 Jan 2009 15:45:09
 *
 *
 */
public class DivertBindingImpl implements DivertBinding
{
   private SimpleString address;
   
   private Divert divert;
   
   public DivertBindingImpl(final SimpleString address, final Divert divert)
   {
      this.address = address;
      
      this.divert = divert;  
   }
        
   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {      
      return divert;
   }
   
   public Divert getDivert()
   {
      return divert;
   }

   public boolean isQueueBinding()
   {
      return false;
   }
}
