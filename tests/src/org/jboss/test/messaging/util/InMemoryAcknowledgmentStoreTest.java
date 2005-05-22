/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.messaging.core.util.InMemoryAcknowledgmentStore;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InMemoryAcknowledgmentStoreTest extends SingleChannelAcknowledgmentStoreTest
{

   // Constructors --------------------------------------------------

   public InMemoryAcknowledgmentStoreTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      acknowledgmentStore = new InMemoryAcknowledgmentStore("InMemoryStoreID");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      acknowledgmentStore = null;
      
   }
   
   //
   // It also runs all tests from AcknowledgmentStoreTest
   //

}
