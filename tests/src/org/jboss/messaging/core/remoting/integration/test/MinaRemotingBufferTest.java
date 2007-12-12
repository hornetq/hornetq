/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.integration.test;

import static java.util.UUID.randomUUID;
import junit.framework.TestCase;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.codec.RemotingBuffer;
import org.jboss.messaging.core.remoting.integration.MinaPacketCodec;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class MinaRemotingBufferTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private RemotingBuffer wrapper;
   private IoBuffer buffer;

   @Override
   protected void setUp() throws Exception
   {
      buffer = IoBuffer.allocate(256);
      buffer.setAutoExpand(true);
      wrapper = new MinaPacketCodec.BufferWrapper(buffer);
   }

   @Override
   protected void tearDown() throws Exception
   {
      wrapper = null;
      buffer = null;

   }

   public void testNullString() throws Exception
   {
      assertNull(putAndGetNullableString(null));
   }

   public void testEmptyString() throws Exception
   {
      String result = putAndGetNullableString("");

      assertNotNull(result);
      assertEquals("", result);
   }

   public void testNonEmptyString() throws Exception
   {
      String junk = randomUUID().toString();

      String result = putAndGetNullableString(junk);

      assertNotNull(result);
      assertEquals(junk, result);
   }

   public void testPutTrueBoolean() throws Exception
   {
      wrapper.putBoolean(true);
      
      buffer.flip();
      
      assertTrue(wrapper.getBoolean());
   }

   public void testPutFalseBoolean() throws Exception
   {
      wrapper.putBoolean(false);
      
      buffer.flip();
      
      assertFalse(wrapper.getBoolean());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String putAndGetNullableString(String nullableString) throws Exception
   {
      wrapper.putNullableString(nullableString);

      buffer.flip();
      
      return wrapper.getNullableString();
   }
   // Inner classes -------------------------------------------------
}
