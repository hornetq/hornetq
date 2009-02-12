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


package org.jboss.messaging.tests.integration.remoting;

import org.jboss.messaging.tests.util.ServiceTestBase;

public class DestroyConsumerTest extends ServiceTestBase
{
   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testFoo()
   {      
   }
   
//   public void testDestroyConsumer() throws Exception
//   {
//      MessagingService service = createService(false, false, createDefaultConfig(), new HashMap<String, AddressSettings>());
//      service.start();
//      
//      SimpleString queue = new SimpleString("add1");
//      
//      ClientSessionFactory factory = createInVMFactory();
//      
//      ClientSession session = factory.createSession(false, false, false, false);
//      
//      session.createQueue(queue, queue, null, false, false);
//      
//      ClientConsumer consumer = session.createConsumer(queue);
//      
//      session.start();
//      
//      Binding binding = service.getServer().getPostOffice().getBindingsForAddress(queue).get(0);
//
//      assertEquals(1, binding.getQueue().getConsumerCount());
//
//      ClientSessionImpl impl = (ClientSessionImpl) session;
//
//      // Simulating a CTRL-C what would close the Socket but not the ClientSession
//      impl.cleanUp();
//      
//      
//      assertEquals(0, binding.getQueue().getConsumerCount());
//      
//      
//      
//   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}
