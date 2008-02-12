/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.bean.Management;
import org.jboss.example.jms.common.bean.ManagementHome;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.api.ClientConnection;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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
   
   public static int getServerID(Connection conn) throws Exception
   {
      if (!(conn instanceof JBossConnection))
      {
         throw new Exception("Connection not an instance of JBossConnection");
      }
      
      JBossConnection jbconn = (JBossConnection)conn;
      
      ClientConnection del = jbconn.getConnection();
      
      return 0;
   }
   
   public static void assertEquals(Object o, Object o2)
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
   
   public static void assertEquals(int i, int i2)
   {
      if (i == i2)
      {
         return;
      }
      
      throw new RuntimeException("Assertion failed, " + i + " != " + i2);
   }
   
   public static void assertNotEquals(int i, int i2)
   {
      if (i != i2)
      {
         return;
      }
      
      throw new RuntimeException("Assertion failed, " + i + " == " + i2);
   }
   
   
   public static void killActiveNode() throws Exception
   {
      // Currently it will always kill the primary node, ignoring nodeID
      
      try
      {
         InitialContext ic = new InitialContext();
         
         ManagementHome home = (ManagementHome)ic.lookup("ejb/Management");
         Management bean = home.create();
         try
         {
            bean.killAS();
         }
         catch(Exception e)
         {
            // OK, I expect exceptions following a VM kill
         }
      }
      catch(Exception e)
      {
         throw new RuntimeException("Could not kill the active node", e);
      }
   }
   
   
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
   
   // Private -------------------------------------------------------
   
   protected void setup() throws Exception
   {
      setup(null);
   }
   
   protected void setup(InitialContext ic) throws Exception
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
      
      if (!Util.doesDestinationExist(jndiDestinationName,ic))
      {
         System.out.println("Destination " + jndiDestinationName + " does not exist, deploying it");
         Util.deployQueue(jndiDestinationName,ic);
         deployed = true;
      }
   }
   
   protected void tearDown() throws Exception
   {
      tearDown(null);
   }
   
   protected void tearDown(InitialContext ic) throws Exception
   {
      if (deployed)
      {
         Util.undeployQueue(jndiDestinationName,ic);
      }
   }
   
   protected void reportResultAndExit()
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
