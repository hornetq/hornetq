/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.MessagingTestCase;
import org.jboss.messaging.interfaces.Router;

import java.util.Iterator;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class DistributorTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public DistributorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testPointToPointRouter() throws Exception
   {
      PointToPointRouter p2pRouter = new PointToPointRouter();
      distributorAddTests(p2pRouter);
      distributorRemoveTests(p2pRouter);
   }

   public void testPointToMultipointRouter() throws Exception
   {
//      PointToMultipointRouter p2mRouter = new PointToMultipointRouter();
//      distributorAddTests(p2mRouter);
//      distributorRemoveTests(p2mRouter);
   }


   /**
    * This method assumes the router has no associates receivers.
    */
   private void distributorAddTests(Router router) throws Exception
   {
      assertFalse(router.iterator().hasNext());

      ReceiverImpl r = new ReceiverImpl();
      assertTrue(router.add(r));
      assertFalse(router.acknowledged(r));

      assertTrue(router.handle(new CoreMessage("")));
      assertTrue(router.acknowledged(r));

      // make sure that a spurious add attempt does not change the status of the last handle() call
      assertFalse(router.add(r));
      assertTrue(router.acknowledged(r));

      router.clear();
   }


   /**
    * This method assumes the router has no associates receivers.
    */
   private void distributorRemoveTests(Router router) throws Exception
   {
      assertFalse(router.iterator().hasNext());

      ReceiverImpl r = new ReceiverImpl();
      assertTrue(router.add(r));
      Iterator i = router.iterator();
      assertEquals(r, i.next());
      assertFalse(i.hasNext());

      assertTrue(router.remove(r));

      // make sure that removing an inexistent Receiver returns false
      assertFalse(router.remove(r));

      i = router.iterator();
      assertFalse(i.hasNext());

      router.clear();
   }
}
