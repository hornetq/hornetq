/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.util.ServerDelegate;
import org.jboss.messaging.core.util.RpcServerCall;
import org.jboss.messaging.core.util.ServerResponse;
import org.jboss.messaging.core.util.ServerDelegateResponse;
import org.jboss.messaging.core.util.RpcServer;
import org.jboss.messaging.core.util.RpcServerCall;
import org.jboss.messaging.core.util.ServerDelegate;
import org.jboss.messaging.core.util.ServerDelegateResponse;
import org.jboss.messaging.core.util.ServerResponse;
import org.jboss.jms.util.RendezVous;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;
import java.util.Collection;
import java.util.Iterator;
import java.io.Serializable;

import junit.framework.TestCase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RendezVousTest extends TestCase
{

   // Constructors --------------------------------------------------

   public RendezVousTest(String name)
   {
      super(name);
   }

   // Attributes ----------------------------------------------------

   protected volatile Object result;

   // TestCase overrides --------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      result = null;
   }

   public void tearDown() throws Exception
   {
      result = null;
      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testRendezVous() throws Exception
   {

      final RendezVous rv = new RendezVous();

      new Thread(new Runnable()
      {
         public void run()
         {
            result = rv.get(0);
         }
      }, "Receiver").start();


      while(!rv.isOccupied())
      {
         Thread.sleep(100);
      }

      assertTrue(rv.isOccupied());

      Object o = new Object();

      assertTrue(rv.put(o));

      while(result == null)
      {
         Thread.sleep(100);
      }

      assertTrue(o == result);

   }

   // Inner classes -------------------------------------------------

}
