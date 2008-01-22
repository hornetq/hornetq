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
package org.jboss.messaging.microcontainer.factory;

import java.util.Map;

import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;

/**
 * The connector interface is onlyextended to allow the easy injection of the server invocation handler.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class Connector extends org.jboss.remoting.transport.Connector
{
    private ServerInvocationHandler serverInvocationHandler;

    public Connector()
    {
        super();
    }

    public Connector(String locatorURI)
    {
        super(locatorURI);
    }

    public Connector(InvokerLocator locator)
    {
        super(locator);
    }

    public Connector(Map configuration)
    {
        super(configuration);
    }

    public Connector(String locatorURI, Map configuration)
    {
        super(locatorURI, configuration);
    }

    public Connector(InvokerLocator locator, Map configuration)
    {
        super(locator, configuration);
    }

    protected Connector(boolean isMarshallerConnector)
    {
        super(isMarshallerConnector);    
    }

    public void setInvocationHandler(ServerInvocationHandler handler) throws Exception
    {
        serverInvocationHandler=handler;
    }


    public void start() throws Exception
    {
        super.start();
        super.addInvocationHandler("JMS", serverInvocationHandler);
    }
}
