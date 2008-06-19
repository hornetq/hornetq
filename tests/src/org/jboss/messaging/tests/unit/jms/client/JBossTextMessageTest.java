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

package org.jboss.messaging.tests.unit.jms.client;

import junit.framework.TestCase;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.*;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;

import javax.jms.DeliveryMode;
import javax.jms.TextMessage;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossTextMessageTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String text;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      text = randomString();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testForeignTextMessage() throws Exception
   {
      TextMessage foreignMessage = createNiceMock(TextMessage.class);
      ClientSession session = EasyMock.createNiceMock(ClientSession.class);
      ByteBufferWrapper body = new ByteBufferWrapper(ByteBuffer.allocate(1024));
      ClientMessage clientMessage = new ClientMessageImpl(JBossTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte)4, body);
      expect(session.createClientMessage(EasyMock.anyByte(), EasyMock.anyBoolean(), EasyMock.anyInt(), EasyMock.anyLong(), EasyMock.anyByte())).andReturn(clientMessage);  
      expect(foreignMessage.getJMSDeliveryMode()).andReturn(DeliveryMode.NON_PERSISTENT);
      expect(foreignMessage.getPropertyNames()).andReturn(Collections.enumeration(Collections.EMPTY_LIST));
      expect(foreignMessage.getText()).andReturn(text);
      
      replay(foreignMessage, session);
      
      JBossTextMessage msg = new JBossTextMessage(foreignMessage, session);
      assertEquals(text, msg.getText());
      
      verify(foreignMessage, session);
   }
   
   public void testGetText() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      msg.setText(text);
      assertEquals(text, msg.getText());
   }

   public void testClearBody() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      msg.setText(text);
      assertEquals(text, msg.getText());
      msg.clearBody();
      assertEquals(null, msg.getText());
   }
   
   public void testGetType() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      assertEquals(JBossTextMessage.TYPE, msg.getType());
   }
   
   public void testDoBeforeSend() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      msg.setText(text);
      
      msg.doBeforeSend();

      MessagingBuffer body = msg.getDelegate().getBody();
      String s = body.getNullableString();
      assertEquals(text, s);
   }
   
   public void testDoBeforeReceive() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      assertNull(msg.getText());
      MessagingBuffer body = msg.getDelegate().getBody();
      body.putNullableString(text);
      body.flip();
      
      msg.doBeforeReceive();
      
      assertEquals(text, msg.getText());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
