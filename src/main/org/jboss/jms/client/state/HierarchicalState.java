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

import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.server.Version;

import java.util.Set;

/**
 * Any state that is Hierarchical in nature implements this interface (e.g. a connection has child
 * sessions). Or, a session has child consumers, producers and browsers.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface HierarchicalState
{
   Set getChildren();
   
   DelegateSupport getDelegate();
   void setDelegate(DelegateSupport delegate);

   HierarchicalState getParent();
   void setParent(HierarchicalState parent);

   Version getVersionToUse();
}
