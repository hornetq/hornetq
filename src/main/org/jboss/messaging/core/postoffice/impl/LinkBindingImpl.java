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
 * A LinkBindingImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 9 Jan 2009 18:29:24
 *
 *
 */
public class LinkBindingImpl implements DivertBinding
{
   private final SimpleString address;
   
   private final Divert link;
   
   public LinkBindingImpl(final SimpleString address, final Divert link)
   {
      this.address = address;
      
      this.link = link;      
   }     
  
   public SimpleString getAddress()
   {
      return address;
   }

   public Bindable getBindable()
   {
      return link;
   }

   public boolean isQueueBinding()
   {
      return false;
   }

   public Divert getDivert()
   {      
      return link;
   }
}
