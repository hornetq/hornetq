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
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientBrowserImpl;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ClientBrowserImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientBrowserImplTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ClientBrowserImplTest.class);

   public void testConstructor() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      EasyMock.replay(session, connection, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, 67567576);
      
      EasyMock.verify(session, connection, rc);            
   }
   
   public void testHasNextMessage() throws Exception
   {
      testHasNextMessage(true);
      testHasNextMessage(false);
   }
   
   public void testNextMessage() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final long targetID = 187282;
      
      long sessionTargetID = 198271982;
      
      ReceiveMessage resp = new ReceiveMessage();
      
      EasyMock.expect(session.getServerTargetID()).andReturn(sessionTargetID);
      
      EasyMock.expect(rc.sendBlocking(targetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE))).andReturn(resp);
      
      EasyMock.replay(session, connection, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, targetID);
            
      ClientMessage msg2 = browser.nextMessage();
      
      assertTrue(msg2 == null);
      
      EasyMock.verify(session, connection, rc);            
   }
   
   public void testReset() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final long targetID = 187282;
      
      long sessionTargetID = 198271982;
      
      EasyMock.expect(session.getServerTargetID()).andReturn(sessionTargetID);
      
      EasyMock.expect(rc.sendBlocking(targetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_RESET))).andReturn(null);
      
      EasyMock.replay(session, connection, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, targetID);
            
      browser.reset();
      
      EasyMock.verify(session, connection, rc);            
   }
   
   public void testClose() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      EasyMock.replay(session, connection, rc);
      
      final long targetID = 187282;
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, targetID);
      
      EasyMock.verify(session, connection, rc);  
      
      EasyMock.reset(session, connection, rc);
      
      long sessionTargetID = 198271982;
      
      EasyMock.expect(session.getServerTargetID()).andReturn(sessionTargetID);
      
      EasyMock.expect(rc.sendBlocking(targetID, sessionTargetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
      
      session.removeBrowser(browser);
      
      EasyMock.replay(session, connection, rc);
      
      assertFalse(browser.isClosed());
            
      browser.close();
      
      EasyMock.verify(session, connection, rc);
      
      assertTrue(browser.isClosed());
      
      //Try and close again - nothing should happen
      
      EasyMock.reset(session, connection, rc);
      
      EasyMock.replay(session, connection, rc);
      
      browser.close();
                
      EasyMock.verify(session, connection, rc);
   }

   // Private -----------------------------------------------------------------------------------------------------------
   
   private void testHasNextMessage(final boolean hasNext) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);
      
      final long targetID = 187282;
      
      long sessionTargetID = 198271982;
      
      SessionBrowserHasNextMessageResponseMessage resp = new SessionBrowserHasNextMessageResponseMessage(hasNext);
      
      EasyMock.expect(session.getServerTargetID()).andReturn(sessionTargetID);
      
      EasyMock.expect(rc.sendBlocking(targetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE))).andReturn(resp);
      
      EasyMock.replay(session, connection, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, targetID);
            
      boolean has = browser.hasNextMessage();
      
      assertEquals(has, hasNext);
      
      EasyMock.verify(session, connection, rc);            
   }

   public void testCleanUp() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      ClientConnectionInternal connection = EasyMock.createStrictMock(ClientConnectionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      EasyMock.expect(session.getConnection()).andReturn(connection);
      EasyMock.expect(connection.getRemotingConnection()).andReturn(rc);

      EasyMock.replay(session, connection, rc);

      ClientBrowser browser =
         new ClientBrowserImpl(session, 67567576);
      EasyMock.verify(session, connection, rc);
      EasyMock.reset(session, connection, rc);
      session.removeBrowser(browser);
      EasyMock.replay(session, connection, rc);
      browser.cleanUp();
      EasyMock.verify(session, connection, rc);

   }
}
