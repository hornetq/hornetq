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
package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.Acceptor;
import org.jboss.messaging.core.remoting.AcceptorFactory;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.invm.INVMAcceptor;
import org.jboss.messaging.core.remoting.impl.mina.MinaAcceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AcceptorFactoryImpl implements AcceptorFactory
{
   public List<Acceptor> createAcceptors(Configuration configuration)
   {
      List<Acceptor> acceptors = new ArrayList<Acceptor>();
      if (TransportType.TCP.equals(configuration.getTransport()))
      {
         acceptors.add(new MinaAcceptor());
         if (!configuration.getConnectionParams().isInVMDisabled())
         {
            acceptors.add(new INVMAcceptor());
         }
      }
      else if (TransportType.INVM.equals(configuration.getTransport()))
      {
         acceptors.add(new INVMAcceptor());
      }
      return acceptors;
   }
}
