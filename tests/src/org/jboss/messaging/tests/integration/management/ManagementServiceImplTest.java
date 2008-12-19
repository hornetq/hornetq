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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.core.security.CheckType.CREATE;
import static org.jboss.messaging.core.security.CheckType.READ;
import static org.jboss.messaging.core.security.CheckType.WRITE;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.util.SimpleString;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementServiceImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testHandleManagementMessageWithAttribute() throws Exception
   {
      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(), false);
      assertNotNull(managementService);

      SimpleString address = RandomUtil.randomSimpleString();
      managementService.registerAddress(address);

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = new ByteBufferWrapper(ByteBuffer.allocate(2048));
      message.setBody(body);
      ManagementHelper.putAttributes(message, ManagementServiceImpl.getAddressObjectName(address), "Address");

      managementService.handleMessage(message);

      SimpleString value = (SimpleString)message.getProperty(new SimpleString("Address"));
      assertNotNull(value);
      assertEquals(address, value);
   }

   public void testHandleManagementMessageWithOperation() throws Exception
   {
      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(), false);
      assertNotNull(managementService);

      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean());

      AddressControlMBean resource = createMock(AddressControlMBean.class);
      resource.addRole(role.getName(), role.isCheckType(CREATE), role.isCheckType(READ), role.isCheckType(WRITE));
      replay(resource);

      SimpleString address = RandomUtil.randomSimpleString();
      ObjectName on = ManagementServiceImpl.getAddressObjectName(address);
      managementService.registerResource(on, resource);

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = new ByteBufferWrapper(ByteBuffer.allocate(2048));
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              on,
                                              "addRole",
                                              role.getName(),
                                              role.isCheckType(CREATE),
                                              role.isCheckType(READ),
                                              role.isCheckType(WRITE));

      managementService.handleMessage(message);

      boolean success = (Boolean)message.getProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED);
      assertTrue(success);

      verify(resource);
   }

   public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(), false);
      assertNotNull(managementService);

      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean());

      String exceptionMessage = randomString();
      AddressControlMBean resource = createMock(AddressControlMBean.class);
      resource.addRole(role.getName(), role.isCheckType(CREATE), role.isCheckType(READ), role.isCheckType(WRITE));
      expectLastCall().andThrow(new Exception(exceptionMessage));
      replay(resource);

      SimpleString address = RandomUtil.randomSimpleString();
      ObjectName on = ManagementServiceImpl.getAddressObjectName(address);
      managementService.registerResource(on, resource);

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = new ByteBufferWrapper(ByteBuffer.allocate(2048));
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              on,
                                              "addRole",
                                              role.getName(),
                                              role.isCheckType(CREATE),
                                              role.isCheckType(READ),
                                              role.isCheckType(WRITE));

      managementService.handleMessage(message);

      boolean success = (Boolean)message.getProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED);
      assertFalse(success);
      SimpleString exceptionMsg = (SimpleString)message.getProperty(ManagementHelper.HDR_JMX_OPERATION_EXCEPTION);
      assertNotNull(exceptionMsg);
      assertEquals(exceptionMessage, exceptionMsg.toString());

      verify(resource);
   }
   
   public void testStop() throws Exception
   {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      
      ManagementService managementService = new ManagementServiceImpl(mbeanServer, true);
      assertNotNull(managementService);

      managementService.registerAddress(randomSimpleString());
      
      assertEquals(1, mbeanServer.queryMBeans(ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":*"), null).size());
      
      managementService.stop();

      assertEquals(0, mbeanServer.queryMBeans(ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":*"), null).size());
   }

   // Package protected ---------------------------------------------
   
   @Override
   protected void tearDown() throws Exception
   {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      assertEquals(0, mbeanServer.queryMBeans(ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":*"), null).size());

      super.tearDown();
   }
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
