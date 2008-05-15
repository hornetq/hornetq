/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.jboss.messaging.util.UUIDGenerator;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class UUIDTest extends TestCase
{
   // Constants -----------------------------------------------------

   private static final int MANY_TIMES = 100000;
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testManyUUIDs() throws Exception
   {
      List<String> uuids = new ArrayList<String>(MANY_TIMES);
      
      UUIDGenerator gen = UUIDGenerator.getInstance();
      InetAddress addr = InetAddress.getLocalHost();
      
      for (int i = 0; i < MANY_TIMES; i++)
      {
         uuids.add(gen.generateTimeBasedUUID(addr).toString());
      }
      
      // we sort the UUIDs...
      Collections.sort(uuids);
      // ...and put them in a set to check duplicates
      Set<String> uuidsSet = new HashSet<String>(uuids);
      
      assertEquals(MANY_TIMES, uuidsSet.size());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
