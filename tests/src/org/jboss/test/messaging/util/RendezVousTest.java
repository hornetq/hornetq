/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.jms.util.RendezVous;

import java.util.List;
import java.util.ArrayList;

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


   public void testRendezVousStress() throws Exception
   {
      final RendezVous rv = new RendezVous();

      int count = 100;
      final List sent = new ArrayList();
      for(int i = 0; i < count; i++)
      {
         sent.add(new Integer(i));
      }
      List received = new ArrayList();

      new Thread(new Runnable()
      {
         public void run()
         {
            // give the receiver thread enough thime to block
            try
            {
               Thread.sleep(500);
            }
            catch(Exception e)
            {
               e.printStackTrace();
            }

            int i = 0;
            while(i < sent.size())
            {
               if (rv.put(sent.get(i)))
               {
                  i++;
               }
            }
         }
      }, "Sender").start();

      while(true)
      {
         result = rv.get(3000);
         if (result == null)
         {
            break;
         }

         // simulate lengthy processing
         Thread.sleep(100);

         received.add(result);
      }


      assertEquals(sent.size(), received.size());
      for(int i = 0; i < sent.size(); i++)
      {
         assertEquals(sent.get(i), received.get(i));
      }

   }

   // Inner classes -------------------------------------------------

}
