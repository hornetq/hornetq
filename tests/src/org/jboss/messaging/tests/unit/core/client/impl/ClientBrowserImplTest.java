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

package org.jboss.messaging.tests.unit.core.client.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientBrowserImpl;
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
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      final long serverTargetID = 1209102;
      final long sessionTargetID = 19281982;
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);      
   }
   
   public void testHasNextMessage() throws Exception
   {
      testHasNextMessage(true);
      testHasNextMessage(false);
   }
   
   public void testNextMessage() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);      
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      final long serverTargetID = 1209102;
      final long sessionTargetID = 19281982;
      
      ReceiveMessage resp = new ReceiveMessage();
      
      EasyMock.expect(rc.sendBlocking(serverTargetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE))).andReturn(resp);
      
      EasyMock.replay(session, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);      
            
      ClientMessage msg2 = browser.nextMessage();
      
      assertTrue(msg2 == null);
      
      EasyMock.verify(session, rc);            
   }
   
   public void testReset() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      final long serverTargetID = 1209102;
      final long sessionTargetID = 19281982;
            
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);  
      
      EasyMock.expect(rc.sendBlocking(serverTargetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_RESET))).andReturn(null);
      
      EasyMock.replay(session, rc);
      
      browser.reset();
      
      EasyMock.verify(session, rc);            
   }
   
   public void testClose() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);     
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      final long serverTargetID = 1209102;
      final long sessionTargetID = 19281982;
            
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);  
       
      EasyMock.expect(rc.sendBlocking(serverTargetID, sessionTargetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
      
      session.removeBrowser(browser);
      
      EasyMock.replay(session, rc);
      
      assertFalse(browser.isClosed());
            
      browser.close();
      
      EasyMock.verify(session, rc);
      
      assertTrue(browser.isClosed());
      
      //Try and close again - nothing should happen
      
      EasyMock.reset(session, rc);
      
      EasyMock.replay(session, rc);
      
      browser.close();
                
      EasyMock.verify(session, rc);
   }

   // Private -----------------------------------------------------------------------------------------------------------
   
   private void testHasNextMessage(final boolean hasNext) throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);     
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);
            
      final long serverTargetID = 187282;
      
      final long sessionTargetID = 198271982;
      
      SessionBrowserHasNextMessageResponseMessage resp = new SessionBrowserHasNextMessageResponseMessage(hasNext);
      
      EasyMock.expect(rc.sendBlocking(serverTargetID, sessionTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE))).andReturn(resp);
      
      EasyMock.replay(session, rc);
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);      
            
      boolean has = browser.hasNextMessage();
      
      assertEquals(has, hasNext);
      
      EasyMock.verify(session, rc);            
   }

   public void testCleanUp() throws Exception
   {
      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
      RemotingConnection rc = EasyMock.createStrictMock(RemotingConnection.class);

      final long serverTargetID = 187282;
      
      final long sessionTargetID = 198271982;
      
      ClientBrowser browser =
         new ClientBrowserImpl(session, serverTargetID, rc, sessionTargetID);      
      
      session.removeBrowser(browser);
      
      EasyMock.replay(session, rc);
      
      browser.cleanUp();
      
      EasyMock.verify(session, rc);
   }
}
