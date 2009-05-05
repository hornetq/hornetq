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

package org.jboss.messaging.tests.integration.cluster.management;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createAddressControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import org.jboss.messaging.utils.SimpleString;

import javax.management.openmbean.TabularData;

/**
 * A ReplicationAwareQueueControlWrapperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ReplicationAwareAddressControlWrapperTest extends ReplicationAwareTestBase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString address;

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAddRole() throws Exception
   {
      AddressControlMBean liveAddressControl = createAddressControl(address, liveMBeanServer);
      AddressControlMBean backupAddressControl = createAddressControl(address, backupMBeanServer);

      Object[] roles = liveAddressControl.getRoles();
      assertEquals(roles.length, backupAddressControl.getRoles().length);

      // add a role
      liveAddressControl.addRole(randomString(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      assertEquals(roles.length + 1, liveAddressControl.getRoles().length);
   }

   public void testRemoveRole() throws Exception
   {
      String roleName = randomString();

      AddressControlMBean liveAddressControl = createAddressControl(address, liveMBeanServer);
      AddressControlMBean backupAddressControl = createAddressControl(address, backupMBeanServer);

      Object[] roles = liveAddressControl.getRoles();
      assertEquals(roles.length, backupAddressControl.getRoles().length);

      // add a role
      liveAddressControl.addRole(roleName, randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      assertEquals(roles.length + 1, liveAddressControl.getRoles().length);
      assertEquals(roles.length + 1, backupAddressControl.getRoles().length);

      // and remove it
      liveAddressControl.removeRole(roleName);

      assertEquals(roles.length, liveAddressControl.getRoles().length);
      assertEquals(roles.length, backupAddressControl.getRoles().length);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      address = randomSimpleString();
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()),
                                                                     new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                backupParams));

      session = sf.createSession(false, true, true);
      session.createQueue(address, address, null, false);
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.deleteQueue(address);
      session.close();

      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
