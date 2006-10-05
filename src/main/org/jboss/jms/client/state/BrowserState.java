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

import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.server.Version;
import org.jboss.jms.client.delegate.DelegateSupport;

/**
 * State corresponding to a browser
 * This state is acessible inside aspects/interceptors
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserState extends HierarchicalStateSupport
{

   private SessionState parent;
   private BrowserDelegate delegate;

   public BrowserState(SessionState parent, BrowserDelegate delegate)
   {
      super(parent, (DelegateSupport)delegate);
   }



    public DelegateSupport getDelegate()
    {
        return (DelegateSupport)delegate;
    }
    public void setDelegate(DelegateSupport delegate)
    {
        this.delegate=(BrowserDelegate)delegate;
    }


   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }

    public void setParent(HierarchicalState parent)
    {
        this.parent=(SessionState)parent;
    }
    public HierarchicalState getParent()
    {
        return parent;
    }

}

