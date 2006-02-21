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
package org.jboss.jms.server.container;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class InjectionAspect
{
    // Constants -----------------------------------------------------
   
    private static final Logger log = Logger.getLogger(InjectionAspect.class);
   
    // Static --------------------------------------------------------

    // Attributes ----------------------------------------------------

    // Constructors --------------------------------------------------

    // Public --------------------------------------------------------

    // Interceptor implementation ------------------------------------

    public Object handleCreateConnectionDelegate(Invocation invocation) throws Throwable
    {
       MethodInvocation mi = (MethodInvocation)invocation;
       
       //First we inject the callback client for the connection
       
       ServerInvokerCallbackHandler handler =
          (ServerInvokerCallbackHandler)mi.getMetaData(MetaDataConstants.JMS,
                                                 MetaDataConstants.CALLBACK_HANDLER);
       
       if (handler == null)
       {
          throw new IllegalStateException("Can't find handler");
       }
       
       ClientConnectionDelegate del = (ClientConnectionDelegate)invocation.invokeNext();
       
       ConnectionAdvised advised = 
          (ConnectionAdvised)JMSDispatcher.instance.getRegistered(new Integer(del.getID()));
       
       ServerConnectionEndpoint endpoint = (ServerConnectionEndpoint)advised.getEndpoint();
       
       endpoint.setCallbackClient(handler.getCallbackClient());
       
       //Then we inject the remoting session id of the client
       String sessionId =
          (String)mi.getMetaData(MetaDataConstants.JMS,
                                 MetaDataConstants.REMOTING_SESSION_ID);
       
       if (sessionId == null)
       {
          throw new IllegalStateException("Can't find session id");
       }
       
       endpoint.setRemotingClientSessionId(sessionId);
       
       return del;
    }
    
    // Package protected ---------------------------------------------

    // Protected -----------------------------------------------------

    // Private -------------------------------------------------------

    // Inner classes -------------------------------------------------
}




