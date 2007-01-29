/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting;

import java.io.Serializable;

import javax.management.ObjectName;

import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.jboss.test.thirdparty.remoting.util.RemotingTestSubsystemService;

/**
 * 
 * A SocketTransportCausalityTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class SocketTransportCausalityTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(SocketTransportCausalityTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InvokerLocator serverLocator;

   // Constructors ---------------------------------------------------------------------------------

   public SocketTransportCausalityTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   //Note!!! before this test is really valid we need to configure the server side to
   //use the DirectThreadPool!!
   public void testOneWayCallsOutOfSequence() throws Throwable
   {
      if (!isRemote())
      {
         fail("This test should be run in a remote configuration!");
      }
 
      final int NUM_THREADS = 4;
      
      final int NUM_INVOCATIONS = 1000;
      
      Sender[] threads = new Sender[NUM_THREADS];
      
      ObjectName subsystemService = null;
      
      try
      {
         subsystemService = RemotingTestSubsystemService.deployService();

         for (int i = 0; i < NUM_THREADS; i++)
         {
            Client client = new Client(serverLocator, RemotingTestSubsystemService.SUBSYSTEM_LABEL);

            client.connect();
            
            Sender sender = new Sender(NUM_INVOCATIONS, client, i);
            
            threads[i] = sender;
         }
         
         for (int i = 0; i < NUM_THREADS; i++)
         {            
            threads[i].start();
         }
         
         for (int i = 0; i < NUM_THREADS; i++)
         {            
            threads[i].join();
         }
         
         for (int i = 0; i < NUM_THREADS; i++)
         {            
            if (threads[i].err != null)
            {
               throw threads[i].err;
            }
         }
         
         boolean failed = 
            RemotingTestSubsystemService.isFailed(subsystemService);
         
         if (failed)
         {
            fail("Invocations received out of sequence");
         }
          
      }
      finally
      {
         for (int i = 0; i < NUM_THREADS; i++)
         {
            threads[i].join(10000);
         }

         RemotingTestSubsystemService.undeployService(subsystemService);
      }
   }

   private static class Sender extends Thread
   {
      int numInvocations;
      
      Client client;
      
      Throwable err;
      
      int num;
      
      int clientNumber;
      
      Sender(int numInvocations, Client client, int clientNumber)
      {
         this.numInvocations = numInvocations;
         
         this.client = client;
         
         this.clientNumber = clientNumber;
      }
      
      public void run()
      {
         try
         {
            for (int i = 0; i < this.numInvocations; i++)
            {
               SimpleInvocation inv = new SimpleInvocation();
               
               inv.clientNumber = clientNumber;
               
               inv.num = ++num;
               
               client.invokeOneway(inv);
            }
         }
         catch (Throwable t)
         {
            err = t;
         }
         finally
         {
            try
            {
               client.disconnect();
            }
            catch (Throwable ignore)
            {               
            }
         }
      }
      
   }
   
   public static class SimpleInvocation implements Serializable
   {
      public int clientNumber;
      
      public int num;
   }


   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start(0, "remoting", null, true, false);

      String s = (String)ServerManagement.
         getAttribute(ServiceContainer.REMOTING_OBJECT_NAME, "InvokerLocator");
      
      //Hmmm adding this doesn't seem to make any difference to what thread pool
      //is used on the server side......
      s += "&onewayThreadPool=org.jboss.jms.server.remoting.DirectThreadPool";

      log.info("Locator is " + s);
      
      serverLocator = new InvokerLocator(s);
      
      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      serverLocator = null;

      ServerManagement.stop();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


}

