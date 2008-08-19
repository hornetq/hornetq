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

package org.jboss.messaging.tests.unit.jms;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;

import javax.jms.Session;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.jms.JBossTemporaryTopic;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossTemporaryTopicTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsTemporary() throws Exception
   {
      String topicName = randomString();
      JBossConnection connection = new JBossConnection("username", "password", 
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, EasyMock.createMock(ClientSessionFactory.class));

      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, EasyMock.createMock(ClientSession.class),
            JBossSession.TYPE_TOPIC_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session,
            topicName);
      assertEquals(true, tempTopic.isTemporary());
   }

   public void testDelete() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);

      ClientSession clientSession = createStrictMock(ClientSession.class);
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(clientSession.bindingQuery(topicAddress)).andReturn(resp);
      clientSession.removeDestination(topicAddress, false);

      replay(clientSession);

      JBossConnection connection = new JBossConnection("username", "password",
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, EasyMock.createMock(ClientSessionFactory.class));
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session,
            topicName);
      tempTopic.delete();

      verify(clientSession);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
