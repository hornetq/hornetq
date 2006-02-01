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

import java.io.Serializable;

import org.jboss.jms.message.MessageDelegate;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;

/**
 * A PooledExecutor job that contains the message to be delivered asynchronously to the client. The
 * delivery is always carried on a thread pool thread.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DeliveryRunnable implements Runnable, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 8375144805659344430L;

   private static final Logger log = Logger.getLogger(DeliveryRunnable.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace;
   
   private MessageDelegate msg;
   
   private transient Client callbackClient;
   
   private int consumerID;
   
   // Constructors --------------------------------------------------

   public DeliveryRunnable(MessageDelegate msg, Client callbackClient, int consumerID, boolean trace)
   {
      this.msg = msg;
      
      this.callbackClient = callbackClient;
      
      this.consumerID = consumerID;
      
      this.trace = trace;
   }

   // Runnable implementation ---------------------------------------

   public void run()
   {
      try
      {
         if (trace) { log.trace("handing " + this.msg + " over to the remoting layer"); }
         
         callbackClient.invoke(this);         
      }
      catch(Throwable t)
      {
         log.warn("Failed to deliver the message to the client, perhaps the client consumer has closed", t);         
      }
   }

   // Public --------------------------------------------------------
   
   public MessageDelegate getMessageDelegate()
   {
      return msg;
   }
   
   public int getConsumerID()
   {
      return consumerID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
