/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.Pipe;
import org.jboss.messaging.core.CoreMessage;
import org.jboss.test.messaging.core.ReceiverImpl;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.util.RpcServer;
import org.jboss.messaging.util.NoSuchServerException;


import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RpcServerTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public RpcServerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testNoSuchServer() throws Exception
   {
      RpcServer rpcServer = new RpcServer();

      try
      {
         rpcServer.invoke("noSuchServerID", "noSuchMethod", new Object[] {}, new String[] {});
         fail("Should have thrown exception");
      }
      catch(NoSuchServerException e)
      {
         // OK
      }
   }

   public void testArgumentClassNotFound() throws Exception
   {
      RpcServer rpcServer = new RpcServer();
      rpcServer.register("serverID1", new ServerObject());

      try
      {
         rpcServer.invoke("serverID1",
                          "someMethod",
                          new Object[] { new Integer(0)},
                          new String[] { "java.lang.NoSuchClass"});
         fail("Should have thrown exception");
      }
      catch(ClassNotFoundException e)
      {
         // OK
      }
   }

   public void testNoSuchMethodName() throws Exception
   {
      RpcServer rpcServer = new RpcServer();
      rpcServer.register("serverID1", new ServerObject());

      try
      {
         rpcServer.invoke("serverID1",
                          "noSuchMethod",
                          new Object[] { new Integer(0)},
                          new String[] { "java.lang.Integer"});
         fail("Should have thrown exception");
      }
      catch(NoSuchMethodException e)
      {
         // OK
      }
   }

   public void testNoSuchMethodSignature() throws Exception
   {
      RpcServer rpcServer = new RpcServer();
      rpcServer.register("serverID1", new ServerObject());

      try
      {
         rpcServer.invoke("serverID1",
                          "methodWithOneIntegerArgument",
                          new Object[] { new Float(0)},
                          new String[] { "java.lang.Float"});
         fail("Should have thrown exception");
      }
      catch(NoSuchMethodException e)
      {
         // OK
      }
   }


   // Inner classes -------------------------------------------------

   private class ServerObject
   {
      public void someMethod() {}

      public void methodWithOneIntegerArgument(Integer i)
      {
      }
   }

}
