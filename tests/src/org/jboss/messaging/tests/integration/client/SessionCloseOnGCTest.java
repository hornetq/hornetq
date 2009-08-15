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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

public class SessionCloseOnGCTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SessionCloseOnGCTest.class);

   private MessagingServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   public void testCloseOneSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl)createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);
      
      assertEquals(1, server.getRemotingService().getConnections().size());
      
      session = null;

      System.gc();
      System.gc();
      System.gc();
      
      Thread.sleep(2000);
            
      assertEquals(0, sf.numSessions());
      assertEquals(0, sf.numConnections());           
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
   public void testCloseSeveralSessionOnGC() throws Exception
   {
      ClientSessionFactoryImpl sf = (ClientSessionFactoryImpl)createInVMFactory();

      ClientSession session1 = sf.createSession(false, true, true);
      ClientSession session2 = sf.createSession(false, true, true);
      ClientSession session3 = sf.createSession(false, true, true);
      
      assertEquals(3, server.getRemotingService().getConnections().size());
      
      session1 = null;
      session2 = null;
      session3 = null;

      System.gc();
      System.gc();
      System.gc();
      
      Thread.sleep(2000);
            
      assertEquals(0, sf.numSessions());
      assertEquals(0, sf.numConnections());           
      assertEquals(0, server.getRemotingService().getConnections().size());
   }

}
