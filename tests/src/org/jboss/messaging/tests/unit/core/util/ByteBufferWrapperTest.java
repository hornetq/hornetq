/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import java.nio.ByteBuffer;

import org.jboss.messaging.tests.integration.core.remoting.mina.MessagingBufferTestBase;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ByteBufferWrapperTest extends MessagingBufferTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ByteBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // MessagingBufferTestBase overrides -----------------------------
   
   @Override
   protected MessagingBuffer createBuffer()
   {
      buffer = ByteBuffer.allocate(256);
      return new ByteBufferWrapper(buffer);
   }

   @Override
   protected void flipBuffer()
   {
      buffer.flip();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
