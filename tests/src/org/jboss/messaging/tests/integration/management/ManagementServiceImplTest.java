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

import org.jboss.messaging.core.logging.Logger;
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
      /*MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
      ManagementService managementService = new ManagementServiceImpl(mbeanServer, false);
      assertNotNull(managementService);
      managementService.start();

      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean());

      AddressControlMBean resource = createMock(AddressControlMBean.class);
      resource.addRole(role.getName(), role.isCheckType(CREATE), role.isCheckType(READ), role.isCheckType(WRITE));
      replay(resource);

      SimpleString address = RandomUtil.randomSimpleString();
      ObjectName on = ObjectNames.getAddressObjectName(address);
      managementService.registerResource(on, resource);

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              on,
                                              "addRole",
                                              role.getName(),
                                              role.isCheckType(CREATE),
                                              role.isCheckType(READ),
                                              role.isCheckType(WRITE));

      ServerMessage reply = managementService.handleMessage(message);

      boolean success = (Boolean)reply.getProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED);
      assertTrue(success);

      verify(resource);

      managementService.stop();*/
   }

   /*public void testHandleManagementMessageWithOperationWhichFails() throws Exception
   {
      MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
      ManagementService managementService = new ManagementServiceImpl(mbeanServer, false);
      assertNotNull(managementService);
      managementService.start();

      Role role = new Role(randomString(), randomBoolean(), randomBoolean(), randomBoolean());

      String exceptionMessage = randomString();
      AddressControlMBean resource = createMock(AddressControlMBean.class);
      resource.addRole(role.getName(), role.isCheckType(CREATE), role.isCheckType(READ), role.isCheckType(WRITE));
      expectLastCall().andThrow(new Exception(exceptionMessage));
      replay(resource);

      SimpleString address = RandomUtil.randomSimpleString();
      ObjectName on = ObjectNames.getAddressObjectName(address);
      managementService.registerResource(on, resource);

      // invoke attribute and operation on the server
      ServerMessage message = new ServerMessageImpl();
      MessagingBuffer body = ChannelBuffers.buffer(2048);
      message.setBody(body);
      ManagementHelper.putOperationInvocation(message,
                                              on,
                                              "addRole",
                                              role.getName(),
                                              role.isCheckType(CREATE),
                                              role.isCheckType(READ),
                                              role.isCheckType(WRITE));

      ServerMessage reply = managementService.handleMessage(message);

      boolean success = (Boolean)reply.getProperty(ManagementHelper.HDR_JMX_OPERATION_SUCCEEDED);
      assertFalse(success);
      SimpleString exceptionMsg = (SimpleString)reply.getProperty(ManagementHelper.HDR_JMX_OPERATION_EXCEPTION);
      assertNotNull(exceptionMsg);
      assertEquals(exceptionMessage, exceptionMsg.toString());

      verify(resource);

      managementService.stop();
   }


   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

      Set set = mbeanServer.queryNames(ObjectName.getInstance(ObjectNames.DOMAIN + ":*"), null);

      for (Object objectName : set)
      {
         mbeanServer.unregisterMBean((ObjectName)objectName);
      }
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

      Set set = mbeanServer.queryMBeans(ObjectName.getInstance(ObjectNames.DOMAIN + ":*"), null);

      for (Object obj : set)
      {
         log.info("mbean:" + set);
      }

      assertEquals(0, mbeanServer.queryMBeans(ObjectName.getInstance(ObjectNames.DOMAIN + ":*"), null).size());

      super.tearDown();
   }*/

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
