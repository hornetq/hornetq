/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.timing.core.remoting.impl;

import java.util.HashSet;
import java.util.Set;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 *
 * A RemotingServiceImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RemotingServiceImplTest extends UnitTestCase
{
    public void testScanForFailedConnectionsNonefailed() throws Exception
    {
       ConfigurationImpl config = new ConfigurationImpl();
       final long interval = 100;
       config.getConnectionParams().setPingInterval(interval);
       RemotingServiceImpl remotingService = new RemotingServiceImpl(config);

       RemotingHandler handler = EasyMock.createStrictMock(RemotingHandler.class);
       remotingService.setHandler(handler);

       Set<Object> failed = new HashSet<Object>();

       EasyMock.expect(handler.scanForFailedConnections((long)(1.5 * interval))).andReturn(failed);

       EasyMock.replay(handler);

       remotingService.start();

       Thread.sleep(interval * 2);

       EasyMock.verify(handler);

    }

    public void testScanForFailedConnectionsFailed() throws Exception
    {
       ConfigurationImpl config = new ConfigurationImpl();
       final long interval = 100;
       config.getConnectionParams().setPingInterval(interval);
       RemotingServiceImpl remotingService = new RemotingServiceImpl(config);

       RemotingHandler handler = EasyMock.createStrictMock(RemotingHandler.class);
       remotingService.setHandler(handler);

       Set<Object> failed = new HashSet<Object>();
       failed.add(2L);
       failed.add(3L);

       EasyMock.expect(handler.scanForFailedConnections((long)(1.5 * interval))).andReturn(failed);

       Connection conn1 = EasyMock.createStrictMock(Connection.class);
       Connection conn2 = EasyMock.createStrictMock(Connection.class);
       Connection conn3 = EasyMock.createStrictMock(Connection.class);

       EasyMock.expect(conn1.getID()).andStubReturn(1);
       EasyMock.expect(conn2.getID()).andStubReturn(2);
       EasyMock.expect(conn3.getID()).andStubReturn(3);

       conn2.close();
       conn3.close();

       class Listener implements FailureListener
       {
          volatile MessagingException me;
          public void connectionFailed(MessagingException me)
          {
             this.me = me;
          }
       }

       EasyMock.replay(handler, conn1, conn2, conn3);

       remotingService.start();

       remotingService.connectionCreated(conn1);
       remotingService.connectionCreated(conn2);
       remotingService.connectionCreated(conn3);

       RemotingConnection rc1 = remotingService.getConnection(1);
       RemotingConnection rc2 = remotingService.getConnection(2);
       RemotingConnection rc3 = remotingService.getConnection(3);

       Listener listener1 = new Listener();
       rc1.addFailureListener(listener1);

       Listener listener2 = new Listener();
       rc2.addFailureListener(listener2);

       Listener listener3 = new Listener();
       rc3.addFailureListener(listener3);

       Thread.sleep(interval * 2);

       EasyMock.verify(handler, conn1, conn2, conn3);

       assertNull(listener1.me);
       assertNotNull(listener2.me);
       assertNotNull(listener3.me);

       assertEquals(MessagingException.CONNECTION_TIMEDOUT, listener2.me.getCode());
       assertEquals(MessagingException.CONNECTION_TIMEDOUT, listener3.me.getCode());

    }

}
