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

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionFactoryEndpoint;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.remoting.callback.ServerInvokerCallbackHandler;

/**
 * The interface only exists so the connection factory requests can call through the AOP stack
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public interface ConnectionFactoryInternalEndpoint extends ConnectionFactoryEndpoint
{
   CreateConnectionResult createConnectionDelegate(String username,
                                                   String password,
                                                   int failedNodeID,
                                                   String remotingSessionID,
                                                   String clientVMID,
                                                   byte versionToUse,
                                                   ServerInvokerCallbackHandler callbackHandler)
                                       throws JMSException;
}
