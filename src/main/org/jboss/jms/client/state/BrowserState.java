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
import org.jboss.jms.client.Closeable;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.util.Version;

/**
 * State corresponding to a browser. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserState extends HierarchicalStateSupport
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private SessionState parent;
   private BrowserDelegate delegate;
   private BrowserDelegate proxyDelegate;

   // data used to recreate the Browser in case of failover
   private JBossDestination jmsDestination;
   private String messageSelector;

   // Constructors ---------------------------------------------------------------------------------

   public BrowserState(SessionState parent, BrowserDelegate delegate, BrowserDelegate proxyDelegate,
                       JBossDestination jmsDestination, String selector)
   {
      super(parent, (DelegateSupport)delegate);
      this.proxyDelegate = proxyDelegate;
      this.jmsDestination = jmsDestination;
      this.messageSelector = selector;
   }

   // HierarchicalState implementation -------------------------------------------------------------


   public Closeable getCloseableDelegate()
   {
      return proxyDelegate;
   }

   public DelegateSupport getDelegate()
   {
      return (DelegateSupport)delegate;
   }
   public void setDelegate(DelegateSupport delegate)
   {
      this.delegate=(BrowserDelegate)delegate;
   }

   public void setParent(HierarchicalState parent)
   {
      this.parent = (SessionState)parent;
   }

   public HierarchicalState getParent()
   {
      return parent;
   }

   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }

   // HierarchicalStateSupport overrides -----------------------------------------------------------

   public void synchronizeWith(HierarchicalState ns) throws Exception
   {      
   }

   // Public ---------------------------------------------------------------------------------------

   public org.jboss.jms.destination.JBossDestination getJmsDestination()
   {
      return jmsDestination;
   }

   public String getMessageSelector()
   {
      return messageSelector;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

