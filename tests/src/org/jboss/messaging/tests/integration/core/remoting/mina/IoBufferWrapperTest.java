/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;

import org.apache.mina.common.IoBuffer;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class IoBufferWrapperTest extends MessagingBufferTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private IoBuffer buffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // BufferWrapperBase overrides -----------------------------------
   
   @Override
   protected MessagingBuffer createBuffer()
   {
      buffer = IoBuffer.allocate(512);
      buffer.setAutoExpand(true);
      return new IoBufferWrapper(buffer);
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
