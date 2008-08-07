/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.jms.server.management.impl;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.easymock.classextension.EasyMock;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.impl.ConnectionFactoryControl;
import org.jboss.messaging.tests.util.RandomUtil;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectionFactoryControlTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetName() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(name, control.getName());

      verify(cf, coreCF);
   }

   public void testGetBindings() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(bindings, control.getBindings());

      verify(cf, coreCF);
   }

   public void testGetURL() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      Location location = createMock(Location.class);
      expect(coreCF.getLocation()).andReturn(location);
      replay(cf, coreCF, location);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(location.toString(), control.getURL());

      verify(cf, coreCF, location);
   }

   public void testGetClientID() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      String clientID = randomString();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      expect(cf.getClientID()).andReturn(clientID);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(clientID, control.getClientID());

      verify(cf, coreCF);
   }

   public void testGetDefaultConsumerMaxRate() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      int rate = randomInt();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.getDefaultConsumerMaxRate()).andReturn(rate);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(rate, control.getDefaultConsumerMaxRate());

      verify(cf, coreCF);
   }

   public void testGetDefaultConsumerWindowSize() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      int size = randomInt();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.getDefaultConsumerWindowSize()).andReturn(size);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(size, control.getDefaultConsumerWindowSize());

      verify(cf, coreCF);
   }

   public void testGetDefaultProducerMaxRate() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      int rate = randomInt();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.getDefaultProducerMaxRate()).andReturn(rate);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(rate, control.getDefaultProducerMaxRate());

      verify(cf, coreCF);
   }

   public void testGetDefaultProducerWindowSize() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      int size = randomInt();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.getDefaultProducerWindowSize()).andReturn(size);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(size, control.getDefaultProducerWindowSize());

      verify(cf, coreCF);
   }

   public void testGetDupsOKBatchSize() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      int size = randomInt();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(cf.getDupsOKBatchSize()).andReturn(size);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(size, control.getDupsOKBatchSize());

      verify(cf, coreCF);
   }

   public void testIsDefaultBlockOnAcknowledge() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      boolean blockOnAcknowledge = randomBoolean();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.isDefaultBlockOnAcknowledge())
            .andReturn(blockOnAcknowledge);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(blockOnAcknowledge, control.isDefaultBlockOnAcknowledge());

      verify(cf, coreCF);
   }

   public void testIsDefaultBlockOnNonPersistentSend() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      boolean blockOnNonPersistentSend = randomBoolean();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.isDefaultBlockOnNonPersistentSend()).andReturn(
            blockOnNonPersistentSend);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(blockOnNonPersistentSend, control
            .isDefaultBlockOnNonPersistentSend());

      verify(cf, coreCF);
   }

   public void testIsDefaultBlockOnPersistentSend() throws Exception
   {
      String name = randomString();
      List<String> bindings = new ArrayList<String>();
      bindings.add(randomString());
      bindings.add(randomString());
      boolean blockOnPersistentSend = randomBoolean();

      JBossConnectionFactory cf = createMock(JBossConnectionFactory.class);
      ClientConnectionFactory coreCF = createMock(ClientConnectionFactory.class);
      expect(coreCF.isDefaultBlockOnPersistentSend()).andReturn(
            blockOnPersistentSend);
      replay(cf, coreCF);

      ConnectionFactoryControl control = new ConnectionFactoryControl(cf,
            coreCF, name, bindings);
      assertEquals(blockOnPersistentSend, control
            .isDefaultBlockOnPersistentSend());

      verify(cf, coreCF);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
