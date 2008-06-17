/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;

import javax.jms.DeliveryMode;
import javax.jms.ObjectMessage;

import junit.framework.TestCase;

import org.jboss.messaging.jms.client.JBossObjectMessage;
import org.jboss.messaging.tests.unit.core.remoting.impl.CodecAssert;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossObjectMessageTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Serializable object;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      object = randomString();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testForeignObjectMessage() throws Exception
   {
      ObjectMessage foreignMessage = createNiceMock(ObjectMessage.class);
      expect(foreignMessage.getJMSDeliveryMode()).andReturn(DeliveryMode.NON_PERSISTENT);
      expect(foreignMessage.getPropertyNames()).andReturn(Collections.enumeration(Collections.EMPTY_LIST));
      expect(foreignMessage.getObject()).andReturn(object);
      
      replay(foreignMessage);
      
      JBossObjectMessage msg = new JBossObjectMessage(foreignMessage);
      assertEquals(object, msg.getObject());
      
      verify(foreignMessage);
   }
   
   public void testGetText() throws Exception
   {
      JBossObjectMessage msg = new JBossObjectMessage();
      msg.setObject(object);
      assertEquals(object, msg.getObject());
   }

   public void testClearBody() throws Exception
   {
      JBossObjectMessage msg = new JBossObjectMessage();
      msg.setObject(object);
      assertEquals(object, msg.getObject());
      msg.clearBody();
      assertEquals(null, msg.getObject());
   }
   
   public void testGetType() throws Exception
   {
      JBossObjectMessage msg = new JBossObjectMessage();
      assertEquals(JBossObjectMessage.TYPE, msg.getType());
   }
   
   public void testDoBeforeSend() throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);      
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush();
      byte[] data = baos.toByteArray();

      JBossObjectMessage msg = new JBossObjectMessage();
      msg.setObject(object);
      
      msg.doBeforeSend();

      MessagingBuffer body = msg.getCoreMessage().getBody();
      assertEquals(data.length, body.getInt());
      byte[] bytes = new byte[data.length];
      body.getBytes(bytes);
      
      CodecAssert.assertEqualsByteArrays(data, bytes);
   }
   
   public void testDoBeforeReceive() throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);      
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush();
      byte[] data = baos.toByteArray();
      
      JBossObjectMessage message = new JBossObjectMessage();
      MessagingBuffer body = message.getCoreMessage().getBody();
      body.putInt(data.length);
      body.putBytes(data);
      body.flip();
      
      message.doBeforeReceive();
      
      assertEquals(object, message.getObject());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
