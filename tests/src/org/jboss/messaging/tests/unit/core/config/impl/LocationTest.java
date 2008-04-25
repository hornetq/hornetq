package org.jboss.messaging.tests.unit.core.config.impl;

import static org.jboss.messaging.core.remoting.TransportType.HTTP;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.impl.LocationImpl;

public class LocationTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testINVMLocationEquality() throws Exception
   {
      assertEquals(new LocationImpl(0), new LocationImpl(0));
      assertNotSame(new LocationImpl(0), new LocationImpl(1));
   }

   public void testTCPLocationEquality() throws Exception
   {
      assertEquals(new LocationImpl(TCP, "localhost", 9000), new LocationImpl(TCP, "localhost", 9000));
      assertNotSame(new LocationImpl(TCP, "localhost", 9001), new LocationImpl(TCP, "localhost", 9000));
      assertNotSame(new LocationImpl(TCP, "anotherhost", 9000), new LocationImpl(TCP, "localhost", 9000));
      assertNotSame(new LocationImpl(HTTP, "localhost", 9000), new LocationImpl(TCP, "localhost", 9000));
   }

   public void testTCPAndINVMLocationInequality() throws Exception
   {
      assertNotSame(new LocationImpl(0), new LocationImpl(TCP, "localhost", 9000));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
