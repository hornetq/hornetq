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
package org.jboss.messaging.tests.unit.core.client.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.*;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.Set;

/**
 * 
 * A ClientConnectionImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientConnectionImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientConnectionImplTest.class);

   public void testGetAttributes() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);   

      Version version = new VersionImpl("blah132", 1, 1, 1, 12, "blah1652");

      Location location = new LocationImpl(TransportType.TCP, "sausages");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);

      ClientConnectionInternal conn = new ClientConnectionImpl(cf, 23, rc, version);

      assertTrue(conn.getServerVersion() == version);
      assertTrue(conn.getRemotingConnection() == rc);      
   }

   public void testCreateSession() throws Exception
   {
      testCreateSession(false, false, false, 14526512, false, false, true);
      testCreateSession(true, true, true, 14526512, true, true, true);

      testCreateSession(false, false, false, 14526512, false, false, false);
      testCreateSession(true, true, true, 14526512, true, true, false);
   }
   
   public void testStartStop() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);   
      
      Version version = new VersionImpl("tyfytfytf", 1, 1, 1, 12, "yttyft");

      Location location = new LocationImpl(TransportType.TCP, "ftftf");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);

      final int serverTargetID = 23;
      
      ClientConnectionInternal conn = new ClientConnectionImpl(cf, serverTargetID, rc, version);
      
      rc.sendOneWay(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CONN_START));
      
      EasyMock.expect(rc.sendBlocking(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CONN_STOP))).andReturn(null);
      
      EasyMock.replay(rc);
      
      conn.start();
      
      conn.stop();
      
      EasyMock.verify(rc);            
   }
   
   public void testSetRemotingSessionListener() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);   
      
      Version version = new VersionImpl("tyfytfytf", 1, 1, 1, 12, "yttyft");

      Location location = new LocationImpl(TransportType.TCP, "ftftf");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);

      final int serverTargetID = 23;
      
      ClientConnectionInternal conn = new ClientConnectionImpl(cf, serverTargetID, rc, version);
      
      RemotingSessionListener listener = new RemotingSessionListener()
      {
         public void sessionDestroyed(long sessionID, MessagingException me)
         {            
         }
      };
      
      rc.setRemotingSessionListener(listener);
            
      EasyMock.replay(rc);
      
      conn.setRemotingSessionListener(listener);
      
      EasyMock.verify(rc);            
   }
   
   public void testClose() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);   
      
      Version version = new VersionImpl("tyfytfytf", 1, 1, 1, 12, "yttyft");

      Location location = new LocationImpl(TransportType.TCP, "ftftf");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      
      final int serverTargetID = 23;
      
      ClientConnectionInternal conn = new ClientConnectionImpl(cf, serverTargetID, rc, version);
      
      assertFalse(conn.isClosed());
    
      //Create some sessions
      
      ClientSessionInternal sess1 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      ClientSessionInternal sess2 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      ClientSessionInternal sess3 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      conn.addSession(sess1);
      conn.addSession(sess2);
      conn.addSession(sess3);
      
      sess1.close();
      sess2.close();
      sess3.close();
      
      EasyMock.expect(rc.sendBlocking(serverTargetID, serverTargetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
            
      rc.stop();      
      
      EasyMock.replay(rc, sess1, sess2, sess3);
      
      conn.close();
      
      EasyMock.verify(rc, sess1, sess2, sess3);
      
      assertTrue(conn.isClosed());
      
      //Close again should do nothing
      EasyMock.reset(rc, sess1, sess2, sess3);
      
      EasyMock.replay(rc, sess1, sess2, sess3);
      
      conn.close();
      
      EasyMock.verify(rc, sess1, sess2, sess3);
      
      try
      {
         conn.createClientSession(false, false, false, 65655);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         conn.createClientSession(false, false, false, 545, false, false);
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         conn.start();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         conn.stop();
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
      
      try
      {
         conn.setRemotingSessionListener(new RemotingSessionListener()
         {
            public void sessionDestroyed(long sessionID, MessagingException me)
            {            
            }
         });
         fail("Should throw exception");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
      }
   }
   
   public void testRemoveSession() throws Exception
   {
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);   
      
      Version version = new VersionImpl("tyfytfytf", 1, 1, 1, 12, "yttyft");

      Location location = new LocationImpl(TransportType.TCP, "ftftf");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      
      final int serverTargetID = 23;
      
      ClientConnectionInternal conn = new ClientConnectionImpl(cf, serverTargetID, rc, version);
      
      //Create some sessions
      
      ClientSessionInternal sess1 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      ClientSessionInternal sess2 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      ClientSessionInternal sess3 = EasyMock.createStrictMock(ClientSessionInternal.class);
      
      conn.addSession(sess1);
      conn.addSession(sess2);
      conn.addSession(sess3);
            
      Set<ClientSession> sessions = conn.getSessions();
      assertEquals(3, sessions.size());
      assertTrue(sessions.contains(sess1));
      assertTrue(sessions.contains(sess2));
      assertTrue(sessions.contains(sess3));
      
      conn.removeSession(sess2);
      
      sessions = conn.getSessions();
      assertEquals(2, sessions.size());
      assertTrue(sessions.contains(sess1));     
      assertTrue(sessions.contains(sess3));
      
      conn.removeSession(sess1);
      
      sessions = conn.getSessions();
      assertEquals(1, sessions.size());   
      assertTrue(sessions.contains(sess3));
      
      conn.removeSession(sess3);
      
      sessions = conn.getSessions();
      assertEquals(0, sessions.size());   
   }
               
   // Private -----------------------------------------------------------------------------------------------------------


   private void testCreateSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks,
         final int ackBatchSize, final boolean blockOnAcknowledge,
         final boolean cacheProducers, final boolean useDefaults) throws Exception
   {       
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class); 

      Location location = new LocationImpl(TransportType.TCP, "oranges");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);

      if (useDefaults)
      {
         cf.setDefaultBlockOnAcknowledge(blockOnAcknowledge); 
      }
      else
      {
         cf.setDefaultBlockOnAcknowledge(!blockOnAcknowledge); // Should be ignored
      }

      final int connTargetID = 17267162;

      Version version = new VersionImpl("uqysuyqs", 1, 1, 1, 12, "uqysuays");

      ClientConnection conn = new ClientConnectionImpl(cf, connTargetID, rc, version);

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(xa, autoCommitSends, autoCommitAcks);

      final int sessionTargetID = 12127162;

      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(sessionTargetID);

      EasyMock.expect(rc.sendBlocking(connTargetID, connTargetID, request)).andReturn(response);

      EasyMock.replay(rc);

      ClientSession session;

      if (useDefaults)
      {
         session = conn.createClientSession(xa, autoCommitSends, autoCommitAcks, ackBatchSize);
      }
      else
      {
         session = conn.createClientSession(xa, autoCommitSends, autoCommitAcks, ackBatchSize, blockOnAcknowledge,
               cacheProducers);
      }

      assertEquals(ackBatchSize, session.getLazyAckBatchSize());
      assertEquals(xa, session.isXA());
      assertEquals(autoCommitSends, session.isAutoCommitSends());
      assertEquals(autoCommitAcks, session.isAutoCommitAcks());
      assertEquals(blockOnAcknowledge, session.isBlockOnAcknowledge());

      EasyMock.verify(rc);
   }

   public void testSessionCleanedUp() throws Exception
      {
         RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      Location location = new LocationImpl(TransportType.TCP, "oranges");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);



      final int connTargetID = 17267162;

      Version version = new VersionImpl("uqysuyqs", 1, 1, 1, 12, "uqysuays");

      ClientConnection conn = new ClientConnectionImpl(cf, connTargetID, rc, version);

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(true, true, true);

      final int sessionTargetID = 12127162;

      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(sessionTargetID);

      EasyMock.expect(rc.sendBlocking(connTargetID, connTargetID, request)).andReturn(response);

      EasyMock.replay(rc);
      ClientSession session = conn.createClientSession(true, true, true, 1);
      conn.cleanUp();
      assertTrue(session.isClosed());
      assertTrue(conn.isClosed());
      EasyMock.verify(rc);

      }


   public void testSessionsCleanedUp() throws Exception
      {
         RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      Location location = new LocationImpl(TransportType.TCP, "oranges");

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);



      final int connTargetID = 17267162;

      Version version = new VersionImpl("uqysuyqs", 1, 1, 1, 12, "uqysuays");

      ClientConnection conn = new ClientConnectionImpl(cf, connTargetID, rc, version);

      ConnectionCreateSessionMessage request = new ConnectionCreateSessionMessage(true, true, true);

      final int sessionTargetID = 12127162;

      ConnectionCreateSessionResponseMessage response = new ConnectionCreateSessionResponseMessage(sessionTargetID);

      EasyMock.expect(rc.sendBlocking(connTargetID, connTargetID, request)).andReturn(response).anyTimes();

      EasyMock.replay(rc);
      ClientSession session = conn.createClientSession(true, true, true, 1);
      ClientSession session2 = conn.createClientSession(true, true, true, 2);
      ClientSession session3 = conn.createClientSession(true, true, true, 3);
      conn.cleanUp();
      assertTrue(session.isClosed());
      assertTrue(session2.isClosed());
      assertTrue(session3.isClosed());
      assertTrue(conn.isClosed());
      EasyMock.verify(rc);

      }
}
