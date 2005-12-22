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
package org.jboss.jms.client.state;

import java.util.Set;

import org.jboss.remoting.Client;

/**
 * 
 * Base implementation of HierarchicalState
 * 
 * State is created and maintained by the StateCreationAspect.
 * The state is placed in the meta data for the invocation, so that
 * it is available in any of the interceptors/aspects, this enables
 * each interceptor/aspect to access the state for it's delegate without
 * having to add multiple get/set methods on the delegate API.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class HierarchicalStateBase implements HierarchicalState
{
   protected Set children;
   
   protected HierarchicalState parent;
   
   protected Object delegate;
   
   public HierarchicalStateBase(HierarchicalState parent, Object delegate)
   {      
      this.parent = parent;
      this.delegate = delegate;
      if (parent != null)
      {
         parent.getChildren().add(this);
      }
   }
   
   public Set getChildren()
   {
      return children;
   }
   
   public HierarchicalState getParent()
   {
      return parent;
   }
   
   public Object getDelegate()
   {
      return delegate;
   }
   
}
