/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import static org.jboss.messaging.core.remoting.TransportType.HTTP;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.ServerLocator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ServerLocatorTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testUnknownTransport() throws Exception
   {
      try
      {
         new ServerLocator("whatever://localhost:9090");
         fail("whatever is not a supported transport type");
      } catch (URISyntaxException e)
      {

      }
   }

   public void testTCPTransport() throws Exception
   {
      ServerLocator locator = new ServerLocator(
            "tcp://localhost:9090");

      assertEquals(TCP, locator.getTransport());
      assertEquals("localhost", locator.getHost());
      assertEquals(9090, locator.getPort());
   }

   public void testHTTPTransport() throws Exception
   {
      ServerLocator locator = new ServerLocator(
            "http://localhost:9090");

      assertEquals(HTTP, locator.getTransport());
      assertEquals("localhost", locator.getHost());
      assertEquals(9090, locator.getPort());
   }

   public void testINVMTransport() throws Exception
   {
      ServerLocator locator = new ServerLocator(
            "invm://localhost:9090");

      assertEquals(INVM, locator.getTransport());
      assertEquals("localhost", locator.getHost());
      assertEquals(9090, locator.getPort());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
