/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster.management;

import static org.hornetq.tests.integration.management.ManagementControlHelper.createAddressControl;
import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.utils.SimpleString;

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
      AddressControl liveAddressControl = createAddressControl(address, liveMBeanServer);
      AddressControl backupAddressControl = createAddressControl(address, backupMBeanServer);

      Object[] roles = liveAddressControl.getRoles();
      assertEquals(roles.length, backupAddressControl.getRoles().length);

      // add a role
      liveAddressControl.addRole(randomString(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

      assertEquals(roles.length + 1, liveAddressControl.getRoles().length);
   }

   public void testRemoveRole() throws Exception
   {
      String roleName = randomString();

      AddressControl liveAddressControl = createAddressControl(address, liveMBeanServer);
      AddressControl backupAddressControl = createAddressControl(address, backupMBeanServer);

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
      
      address = null;
      session = null;

      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
