/*
 * JBoss, Home of Professional Open Source
 * 
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors by the
 * 
 * @authors tag. See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementServiceImplTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private final Logger log = Logger.getLogger(ManagementServiceImplTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testHandleManagementMessageWithOperation() throws Exception
   {
      String queue = randomString();
      String address = randomString();
      
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      MessagingServer server = Messaging.newMessagingServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              ResourceNames.CORE_SERVER,
                                              "createQueue",
                                              queue,
                                              address);
      
      ServerMessage reply = server.getManagementService().handleMessage(message);
      
      assertTrue(ManagementHelper.hasOperationSucceeded(reply));

      server.stop();
   }

   public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      MessagingServer server = Messaging.newMessagingServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              ResourceNames.CORE_SERVER,
                                              "thereIsNoSuchOperation");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getOperationExceptionMessage(reply));
      server.stop();
   }
   
   public void testHandleManagementMessageWithUnknowResource() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      MessagingServer server = Messaging.newMessagingServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              "Resouce.Does.Not.Exist",
                                              "toString");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getOperationExceptionMessage(reply));
      server.stop();
   }

   public void testHandleManagementMessageWithUnknowAttribute() throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setJMXManagementEnabled(false);
      
      MessagingServer server = Messaging.newMessagingServer(conf, false);
      server.start();

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putAttribute(message, ResourceNames.CORE_SERVER, "Attribute.Does.Not.Exist");
      
      ServerMessage reply = server.getManagementService().handleMessage(message);

      
      assertFalse(ManagementHelper.hasOperationSucceeded(reply));
      assertNotNull(ManagementHelper.getOperationExceptionMessage(reply));
      server.stop();
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
