/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.local.LocalQueue;
import org.jboss.messaging.core.local.LocalQueue;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalQueueAsDistributorTest extends DistributorTest
{
   // Constructors --------------------------------------------------

   public LocalQueueAsDistributorTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      super.setUp();

      // Create a LocalQueue to be tested by the superclass tests
      distributor = new LocalQueue("LocalQueueID");
   }

   public void tearDown()throws Exception
   {
      distributor.clear();
      distributor = null;
      super.tearDown();
   }

   //
   // This test also runs all DistributorTest's tests
   //

}
