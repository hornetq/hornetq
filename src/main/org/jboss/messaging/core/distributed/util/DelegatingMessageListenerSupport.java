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
package org.jboss.messaging.core.distributed.util;

import org.jgroups.MessageListener;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class DelegatingMessageListenerSupport implements DelegatingMessageListener
{
   // Attributes ----------------------------------------------------

   protected MessageListener delegate;

   // Constructors --------------------------------------------------

   public DelegatingMessageListenerSupport(MessageListener delegate)
   {
      this.delegate = delegate;
   }

   // DelegatingMessageListener implementation ----------------------

   public MessageListener getDelegate()
   {
      return delegate;
   }

   public boolean remove(MessageListener listener)
   {
      if (delegate == null)
      {
         return false;
      }

      if (delegate == listener)
      {
         if (delegate instanceof DelegatingMessageListener)
         {
            // restore the chain
            delegate = ((DelegatingMessageListener)delegate).getDelegate();
         }
         else
         {
            delegate = null;
         }
         return true;
      }
      else if (delegate instanceof DelegatingMessageListener)
      {
         return ((DelegatingMessageListener)delegate).remove(listener);
      }
      return false;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
