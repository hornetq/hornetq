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

import org.jboss.messaging.core.logging.Logger;
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

   public void testDummy()
   {      
   }
   
//   public void testConstructor() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//            
//      final long serverTargetID = 1209102;     
//      
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);      
//   }
//   
//   public void testHasNextMessage() throws Exception
//   {
//      testHasNextMessage(true);
//      testHasNextMessage(false);
//   }
//   
//   public void testNextMessage() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);      
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//            
//      final long serverTargetID = 1209102;
//   
//      ReceiveMessage resp = new ReceiveMessage();
//      
//      EasyMock.expect(cm.sendCommandBlocking(serverTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_NEXTMESSAGE))).andReturn(resp);
//      
//      EasyMock.replay(session, cm);
//      
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);      
//            
//      ClientMessage msg2 = browser.nextMessage();
//      
//      assertTrue(msg2 == null);
//      
//      EasyMock.verify(session, cm);            
//   }
//   
//   public void testReset() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//            
//      final long serverTargetID = 1209102;
//         
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);  
//      
//      EasyMock.expect(cm.sendCommandBlocking(serverTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_RESET))).andReturn(null);
//      
//      EasyMock.replay(session, cm);
//      
//      browser.reset();
//      
//      EasyMock.verify(session, cm);            
//   }
//   
//   public void testClose() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);     
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//            
//      final long serverTargetID = 1209102;
//          
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);  
//       
//      EasyMock.expect(cm.sendCommandBlocking(serverTargetID, new PacketImpl(PacketImpl.CLOSE))).andReturn(null);
//      
//      session.removeBrowser(browser);
//      
//      EasyMock.replay(session, cm);
//      
//      assertFalse(browser.isClosed());
//            
//      browser.close();
//      
//      EasyMock.verify(session, cm);
//      
//      assertTrue(browser.isClosed());
//      
//      //Try and close again - nothing should happen
//      
//      EasyMock.reset(session, cm);
//      
//      EasyMock.replay(session, cm);
//      
//      browser.close();
//                
//      EasyMock.verify(session, cm);
//   }
//
//   // Private -----------------------------------------------------------------------------------------------------------
//   
//   private void testHasNextMessage(final boolean hasNext) throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);     
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//            
//      final long serverTargetID = 187282;
//      
//      SessionBrowserHasNextMessageResponseMessage resp = new SessionBrowserHasNextMessageResponseMessage(hasNext);
//      
//      EasyMock.expect(cm.sendCommandBlocking(serverTargetID, new PacketImpl(PacketImpl.SESS_BROWSER_HASNEXTMESSAGE))).andReturn(resp);
//      
//      EasyMock.replay(session, cm);
//      
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);      
//            
//      boolean has = browser.hasNextMessage();
//      
//      assertEquals(has, hasNext);
//      
//      EasyMock.verify(session, cm);            
//   }
//
//   public void testCleanUp() throws Exception
//   {
//      ClientSessionInternal session = EasyMock.createStrictMock(ClientSessionInternal.class);
//      CommandManager cm = EasyMock.createStrictMock(CommandManager.class);
//
//      final long serverTargetID = 187282;
//      
//      ClientBrowser browser =
//         new ClientBrowserImpl(session, serverTargetID, cm);      
//      
//      session.removeBrowser(browser);
//      
//      EasyMock.replay(session, cm);
//      
//      browser.cleanUp();
//      
//      EasyMock.verify(session, cm);
//   }
}
