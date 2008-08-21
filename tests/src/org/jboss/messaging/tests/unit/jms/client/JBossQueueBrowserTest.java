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
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientBrowser;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.JBossQueueBrowser;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossQueueBrowserTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetMessageSelector() throws Exception
   {
      String messageSelector = "color = 'green'";
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, messageSelector,
            clientBrowser);
      assertEquals(messageSelector, browser.getMessageSelector());

      verify(clientBrowser);
   }

   public void testGetQueue() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);
      assertEquals(queue, browser.getQueue());

      verify(clientBrowser);
   }

   public void testClose() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      clientBrowser.close();
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      browser.close();

      verify(clientBrowser);
   }

   public void testCloseThrowsException() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
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
   }

   public void testGetEnumeration() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      clientBrowser.reset();
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);

      verify(clientBrowser);
   }

   public void testGetEnumerationThrowsException() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      clientBrowser.reset();
      expectLastCall().andThrow(new MessagingException());
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      try
      {
         browser.getEnumeration();
         fail("JMSException");
      } catch (JMSException e)
      {
      }

      verify(clientBrowser);
   }

   public void testGetEnumerationWithOneMessage() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      ClientMessage clientMessage = createStrictMock(ClientMessage.class);
      MessagingBuffer buffer = createStrictMock(MessagingBuffer.class);
      clientBrowser.reset();
      expect(clientBrowser.hasNextMessage()).andReturn(true);
      expect(clientMessage.getType()).andReturn(JBossMessage.TYPE);
      expect(clientMessage.getBody()).andStubReturn(buffer);
      expect(clientBrowser.nextMessage()).andReturn(clientMessage);
      expect(clientBrowser.hasNextMessage()).andReturn(false);
      replay(clientMessage, clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);
      assertTrue(enumeration.hasMoreElements());
      Message message = (Message) enumeration.nextElement();
      assertFalse(enumeration.hasMoreElements());

      verify(clientMessage, clientBrowser);
   }

   public void testGetEnumerationWithHasMoreElementsThrowsException()
         throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      clientBrowser.reset();
      expect(clientBrowser.hasNextMessage()).andThrow(new MessagingException());
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);

      try
      {
         enumeration.hasMoreElements();
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      verify(clientBrowser);
   }

   public void testGetEnumerationWithNextThrowsException() throws Exception
   {
      Queue queue = new JBossQueue(randomString());
      ClientBrowser clientBrowser = createStrictMock(ClientBrowser.class);
      clientBrowser.reset();
      expect(clientBrowser.hasNextMessage()).andReturn(true);
      expect(clientBrowser.nextMessage()).andThrow(new MessagingException());
      replay(clientBrowser);

      JBossQueueBrowser browser = new JBossQueueBrowser(queue, null,
            clientBrowser);

      Enumeration enumeration = browser.getEnumeration();
      assertNotNull(enumeration);
      assertTrue(enumeration.hasMoreElements());

      try
      {
         enumeration.nextElement();
         fail("IllegalStateException");
      } catch (IllegalStateException e)
      {
      }

      verify(clientBrowser);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
