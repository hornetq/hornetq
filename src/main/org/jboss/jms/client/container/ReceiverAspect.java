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
package org.jboss.jms.client.container;

import javax.jms.MessageListener;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;

/**
 * 
 * This aspect handles receive functionality for a message consumer
 * 
 * This aspect is PER_INSTANCE.
 * 
 * (TODO consider merging this with ConsumerInterceptor
 * I don't see the advantage of splitting this functionality out into a
 * different aspect from ConsumerAspect)
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReceiverAspect.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public Object handleReceive(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Object[] args = mi.getArguments();      
      
      long timeout = args == null ? 0 : ((Long)args[0]).longValue();
      
      return getHandler(invocation).receive(timeout);
   }
   
   public Object handleReceiveNoWait(Invocation invocation) throws Throwable
   {      
      return getHandler(invocation).receive(-1);
   }
   
   public Object handleSetMessageListener(Invocation invocation) throws Throwable
   {   
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Object[] args = mi.getArguments();
      
      MessageListener l = (MessageListener)args[0];     
      
      getHandler(invocation).setMessageListener(l);
      
      return null;
   }
   
   public Object handleGetMessageListener(Invocation invocation) throws Throwable
   {       
      return getHandler(invocation).getMessageListener();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private MessageCallbackHandler getHandler(Invocation inv)
   {
      return (MessageCallbackHandler)inv.
         getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.MESSAGE_HANDLER);
   }

   // Inner classes -------------------------------------------------

}
