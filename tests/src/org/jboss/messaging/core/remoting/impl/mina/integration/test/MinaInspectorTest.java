/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.integration.test;

import static org.apache.mina.filter.reqres.ResponseType.WHOLE;

import java.util.UUID;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.mina.MinaInspector;
import org.jboss.messaging.core.remoting.impl.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.impl.wireformat.NullPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaInspectorTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private MinaInspector inspector;

   public void testGetRequestIdForNull()
   {
      assertNull(inspector.getRequestId(null));
   }

   public void testGetRequestIdForNotAbstractPacket()
   {
      assertNull(inspector.getRequestId(new Object()));
   }

   public void testGetRequestIdForAbstractPacketWhichIsNotRequest()
   {
      AbstractPacket packet = new NullPacket();
      packet.setTargetID(UUID.randomUUID().toString());
      assertFalse(packet.isRequest());

      assertNull(inspector.getRequestId(packet));
   }

   public void testGetRequestIdForAbstractPacketWhichIsRequest()
   {
      AbstractPacket packet = new NullPacket();
      packet.setTargetID(UUID.randomUUID().toString());
      packet.setCorrelationID(System.currentTimeMillis());
      assertTrue(packet.isRequest());

      Object requestID = inspector.getRequestId(packet);
      assertNotNull(requestID);
      assertEquals(packet.getCorrelationID(), requestID);
   }

   public void testGetResponseTypeForNull()
   {
      assertNull(inspector.getResponseType(null));
   }

   public void testGetResponseTypeForNotAbstractPacket()
   {
      assertNull(inspector.getResponseType(new Object()));
   }

   public void testGetResponseTypeForAbstractPacket()
   {
      AbstractPacket packet = new NullPacket();

      assertEquals(WHOLE, inspector.getResponseType(packet));
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      inspector = new MinaInspector();
   }

   @Override
   protected void tearDown() throws Exception
   {
      inspector = null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
