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

package org.jboss.messaging.tests.integration.core.remoting.mina;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATECONNECTION;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.RemotingSession;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.impl.ServerPacketHandlerSupport;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerPingTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private RemotingServiceImpl service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();
      service = null;
   }

   public void testKeepAliveWithServerNotResponding() throws Throwable
   {
      //set the server timeouts to be twice that of the server to force failure
      ConfigurationImpl config = ConfigurationHelper.newTCPConfiguration(
              "localhost", TestSupport.PORT);
      config.getConnectionParams().setPingInterval(TestSupport.PING_INTERVAL * 2);
      config.getConnectionParams().setPingTimeout(TestSupport.PING_TIMEOUT * 2);
      ConfigurationImpl clientConfig = ConfigurationHelper.newTCPConfiguration(
              "localhost", TestSupport.PORT);
      clientConfig.getConnectionParams().setPingInterval(TestSupport.PING_INTERVAL);
      clientConfig.getConnectionParams().setPingTimeout(TestSupport.PING_TIMEOUT);
      service = new RemotingServiceImpl(config);
      service.start();
      service.getDispatcher().register(new DummyServePacketHandler());
      MinaConnector connector = new MinaConnector(clientConfig.getLocation(), clientConfig.getConnectionParams(), new PacketDispatcherImpl(null));

      final AtomicLong sessionIDNotResponding = new AtomicLong(-1);
      final CountDownLatch latch = new CountDownLatch(1);

      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {
            sessionIDNotResponding.set(sessionID);
            latch.countDown();
         }
      };
      connector.addSessionListener(listener);

      RemotingSession session = connector.connect();
      boolean firedKeepAliveNotification = latch.await(TestSupport.PING_INTERVAL
              + TestSupport.PING_TIMEOUT + 2000, MILLISECONDS);
      assertTrue(firedKeepAliveNotification);
      assertEquals(session.getID(), sessionIDNotResponding.longValue());

      connector.removeSessionListener(listener);
      connector.disconnect();
   }

   class DummyServePacketHandler extends ServerPacketHandlerSupport
   {
      public long getID()
      {
         //0 is reserved for this handler
         return 0;
      }

      public Packet doHandle(final Packet packet, final PacketReturner sender) throws Exception
      {
         Packet response = null;

         byte type = packet.getType();

         if (type == CREATECONNECTION)
         {
            /*CreateConnectionRequest request = (CreateConnectionRequest) packet;

            CreateConnectionResponse createConnectionResponse = server.createConnection(request.getUsername(), request.getPassword(),
                    request.getRemotingSessionID(),
                    sender.getRemoteAddress(),
                    request.getVersion(),
                    sender);
            response = createConnectionResponse;*/

         }
         else if (type == PacketImpl.PING)
         {
            //do nothing
         }

         return response;
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   // Inner classes -------------------------------------------------
}