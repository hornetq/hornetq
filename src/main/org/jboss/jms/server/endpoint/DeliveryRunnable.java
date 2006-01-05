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
package org.jboss.jms.server.endpoint;

import javax.jms.Message;

import org.jboss.logging.Logger;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.InvokerCallbackHandler;

/**
 * A PooledExecutor job that contains the message to be delivered asynchronously to the client. The
 * delivery is always carried on a thread pool thread.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class DeliveryRunnable extends Callback implements Runnable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 8375144805659344430L;

   private static final Logger log = Logger.getLogger(DeliveryRunnable.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected transient InvokerCallbackHandler callbackHandler;

   // Constructors --------------------------------------------------

   public DeliveryRunnable(InvokerCallbackHandler callbackHandler,
                           Message m)
   {
      super(m);
      this.callbackHandler = callbackHandler;
   }

   // Runnable implementation ---------------------------------------

   public void run()
   {
      try
      {
         if (log.isTraceEnabled()) { log.trace("handing " + this.getCallbackObject() + " over to the remoting layer"); }
         callbackHandler.handleCallback(this);
      }
      catch(Throwable t)
      {
         log.warn("Failed to deliver the message to the client, perhaps the client consumer has closed", t);         
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
