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
package org.jboss.jms.server.endpoint.delegate;

/**
 * Base class for endpoint delegates.
 * 
 * Endpoint delegates are concrete implementations of the particular
 * endpoint interface that simply delegate all method calls to their corresponding
 * concrete server endpoint class.
 * E.g. SessionEndpointDelegate delegates ServerSessionEndpoint.
 * The endpoint delegates are the classes that get advised by AOP to provide
 * the server side advice stack.
 * We do not advise the actual concrete server endpoint class directly since that
 * would lead to all method invocations for the advised methods also going through the
 * interceptor stack which is unnecessary and unperformant.
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public abstract class EndpointDelegateBase
{
   public abstract Object getEndpoint();
}
