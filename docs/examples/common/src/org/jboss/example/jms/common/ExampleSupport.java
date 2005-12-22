/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common;

import javax.jms.ConnectionMetaData;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class ExampleSupport
 {
   // Constants -----------------------------------------------------

   public static final String DEFAULT_QUEUE_NAME = "testQueue";
   public static final String DEFAULT_TOPIC_NAME = "testTopic";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean failure;
   private boolean deployed;
   private String jndiDestinationName;

   // Constructors --------------------------------------------------

   protected ExampleSupport()
   {
      failure = false;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract void example() throws Exception;
   protected abstract boolean isQueueExample();

   protected final boolean isTopicExample()
   {
      return !isQueueExample();
   }

   protected void run()
   {
      try
      {
         setup();
         example();
         tearDown();
      }
      catch(Throwable t)
      {
         t.printStackTrace();
         setFailure(true);
      }

      reportResultAndExit();
   }

   protected void setFailure(boolean b)
   {
      failure = b;
   }

   protected boolean isFailure()
   {
      return failure;
   }

   protected String getDestinationJNDIName()
   {
      return jndiDestinationName;
   }

   protected void log(String s)
   {
      System.out.println(s);
   }

   protected void displayProviderInfo(ConnectionMetaData metaData) throws Exception
   {
      String info =
            "The example connected to " + metaData.getJMSProviderName() +
            " version " + metaData.getProviderVersion() + " (" +
            metaData.getProviderMajorVersion() + "." + metaData.getProviderMinorVersion() +
            ")";

     System.out.println(info);
   }

   protected void assertEquals(Object o, Object o2)
   {
      if (o == null && o2 == null)
      {
         return;
      }

      if (o.equals(o2))
      {
         return;
      }

      throw new RuntimeException("Assertion failed, " + o + " != " + o2);
   }

   // Private -------------------------------------------------------

   private void setup() throws Exception
   {
      String destinationName;

      if (isQueueExample())
      {
         destinationName = System.getProperty("example.queue.name");
         jndiDestinationName =
            "/queue/"  + (destinationName == null ? DEFAULT_QUEUE_NAME : destinationName);
      }
      else
      {
         destinationName = System.getProperty("example.topic.name");
         jndiDestinationName =
            "/topic/"  + (destinationName == null ? DEFAULT_TOPIC_NAME : destinationName);
      }

      if (!Util.doesDestinationExist(jndiDestinationName))
      {
         Util.deployQueue(jndiDestinationName);
         deployed = true;
      }
   }

   private void tearDown() throws Exception
   {
      if (deployed)
      {
         Util.undeployQueue(jndiDestinationName);
      }
   }

   private void reportResultAndExit()
   {
      if (isFailure())
      {
         System.err.println();
         System.err.println("#####################");
         System.err.println("###    FAILURE!   ###");
         System.err.println("#####################");
         System.exit(1);
      }

      System.out.println();
      System.out.println("#####################");
      System.out.println("###    SUCCESS!   ###");
      System.out.println("#####################");
      System.exit(0);
   }

   // Inner classes -------------------------------------------------

}
