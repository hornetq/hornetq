/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Collections;

import javax.jms.DeliveryMode;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.MessagingBuffer;

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
      expect(foreignMessage.getJMSDeliveryMode()).andReturn(DeliveryMode.NON_PERSISTENT);
      expect(foreignMessage.getPropertyNames()).andReturn(Collections.enumeration(Collections.EMPTY_LIST));
      expect(foreignMessage.getText()).andReturn(text);
      
      replay(foreignMessage);
      
      JBossTextMessage msg = new JBossTextMessage(foreignMessage);
      assertEquals(text, msg.getText());
      
      verify(foreignMessage);
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

      MessagingBuffer body = msg.getCoreMessage().getBody();
      String s = body.getNullableString();
      assertEquals(text, s);
   }
   
   public void testDoBeforeReceive() throws Exception
   {
      JBossTextMessage msg = new JBossTextMessage();
      assertNull(msg.getText());
      MessagingBuffer body = msg.getCoreMessage().getBody();
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
