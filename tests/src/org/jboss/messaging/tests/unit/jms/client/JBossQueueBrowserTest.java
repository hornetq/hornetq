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

package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Enumeration;

import javax.jms.Message;

import org.easymock.EasyMock;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossQueueBrowser;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class JBossQueueBrowserTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetMessageSelector() throws Exception
   {
      String messageSelector = "color = 'green'";
      JBossQueue queue = new JBossQueue(randomString());
      ClientSession clientBrowser = createStrictMock(ClientSession.class);
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, messageSelector,
            clientBrowser);
      assertEquals(messageSelector, browser.getMessageSelector());

      verify(clientBrowser);
   }

   public void testGetQueue() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientSession clientBrowser = createStrictMock(ClientSession.class);
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);
      assertEquals(queue, browser.getQueue());

      verify(clientBrowser);
   }

   public void testClose() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientSession clientBrowser = createStrictMock(ClientSession.class);
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      browser.close();

      verify(clientBrowser);
   }

/*   public void testCloseThrowsException() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientSession clientBrowser = createStrictMock(ClientSession.class);
      clientBrowser.close();
      expectLastCall().andThrow(new MessagingException());

      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      try
      {
         browser.close();
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientBrowser);
   }*/

   public void testGetEnumeration() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientConsumer consumer = createStrictMock(ClientConsumer.class);
      ClientSession session = createStrictMock(ClientSession.class);    
      expect(session.createConsumer((SimpleString) EasyMock.anyObject(), (SimpleString) EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(consumer);
      replay(session, consumer);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            session);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);

      verify(session, consumer);
   }

   public void testGetEnumerationWithOneMessage() throws Exception
   {
      JBossQueue queue = new JBossQueue(randomString());
      ClientMessage clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      ClientConsumer consumer = createStrictMock(ClientConsumer.class);
      ClientSession session = createStrictMock(ClientSession.class);   
      expect(session.createConsumer((SimpleString) EasyMock.anyObject(), (SimpleString) EasyMock.anyObject(), EasyMock.anyBoolean())).andReturn(consumer);
      expect(consumer.receive(1000)).andReturn(clientMessage);
      expect(clientMessage.getType()).andReturn(JBossMessage.TYPE);
      expect(clientMessage.getBody()).andStubReturn(buffer);
      expect(consumer.receive(1000)).andReturn(null);
      replay(clientMessage, session, consumer);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            session);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);
      assertTrue(enumeration.hasMoreElements());
      Message message = (Message) enumeration.nextElement();
      assertFalse(enumeration.hasMoreElements());

      verify(clientMessage, session, consumer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
