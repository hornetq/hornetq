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
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.remoting.callback.InvokerCallbackHandler;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class InjectionAspect
{
    // Constants -----------------------------------------------------

    // Static --------------------------------------------------------

    // Attributes ----------------------------------------------------

    // Constructors --------------------------------------------------

    // Public --------------------------------------------------------

    // Interceptor implementation ------------------------------------

    public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
    {
       MethodInvocation mi = (MethodInvocation)invocation;
       
       InvokerCallbackHandler handler =
          (InvokerCallbackHandler)mi.getMetaData(MetaDataConstants.JMS, MetaDataConstants.CALLBACK_HANDLER);
       if (handler != null)
       {
          SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
          ServerSessionEndpoint endpoint = (ServerSessionEndpoint)del.getEndpoint();
          endpoint.setCallbackHandler(handler);
       }
       
       return invocation.invokeNext();
    }
    
    // Package protected ---------------------------------------------

    // Protected -----------------------------------------------------

    // Private -------------------------------------------------------

    // Inner classes -------------------------------------------------
}




