/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.core.distributed.util;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerFacade;
import org.jboss.messaging.core.distributed.util.RpcServerCall;
import org.jboss.messaging.core.distributed.util.ServerFacade;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.util.ServerResponse;
import org.jboss.messaging.core.distributed.util.SubordinateServerResponse;
import org.jboss.messaging.core.distributed.util.SubordinateServerResponse;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;
import java.util.Collection;
import java.util.Iterator;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RpcServerTest extends MessagingTestCase
{

   private String props =
         "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):"+
         "PING(timeout=3050;num_initial_members=6):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "UNICAST(timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)";

   // Constructors --------------------------------------------------

   RpcServer rpcServer;

   public RpcServerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      rpcServer = new RpcServer();
   }

   public void tearDown() throws Exception
   {
      rpcServer = null;
      super.tearDown();
   }

   //
   // Registration / Unregistration tests
   //

   public void testNullServerObject() throws Exception
   {
      try
      {
         rpcServer.register("whatever", null);
         fail("Should have thrown NullPointerException");
      }
      catch(NullPointerException e)
      {
         // OK
      }
   }

   public void testRegistration() throws Exception
   {
      Subordinate so = new Subordinate();
      assertTrue(rpcServer.register("someCategory", so));
      assertFalse(rpcServer.register("someCategory", so));
      Set s = rpcServer.get("someCategory");
      assertEquals(1, s.size());
      assertTrue(so == s.iterator().next());
   }

   public void testUniqueRegistration() throws Exception
   {
      Subordinate so = new Subordinate();
      assertTrue(rpcServer.registerUnique("someCategory", so));
      assertFalse(rpcServer.register("someCategory", so));
      assertFalse(rpcServer.register("someCategory", new Subordinate()));
      Set s = rpcServer.get("someCategory");
      assertEquals(1, s.size());
      assertTrue(so == s.iterator().next());
   }

   public void testEquivalentFollowedByUniqueRegistration() throws Exception
   {
      Subordinate so = new Subordinate();
      assertTrue(rpcServer.register("someCategory", so));
      assertTrue(rpcServer.unregister("someCategory", so));
      so = new Subordinate();
      assertTrue(rpcServer.registerUnique("someCategory", so));
      Set s = rpcServer.get("someCategory");
      assertEquals(1, s.size());
      assertTrue(so == s.iterator().next());
   }



   public void testEmptyGet() throws Exception
   {
      Set s = rpcServer.get("whatever");
      assertEquals(0, s.size());
   }



   //
   // Invocation tests
   //

   public void testArgumentClassNotFound_Local() throws Exception
   {
      try
      {
         rpcServer.invoke("nosuchcategory",
                          "nosuchmethod",
                          new Object[] { new Integer(0)},
                          new String[] { "org.nosuchpackage.NoSuchClass"});
         fail("Should have thrown exception");
      }
      catch(ClassNotFoundException e)
      {
         // OK
      }
   }

   public void testArgumentClassNotFound_Remote() throws Exception
   {
      RpcServer rpcServer = new RpcServer();
      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());

      RpcServerCall call =
            new RpcServerCall("nosuchcategory",
                              "nosuchmethod",
                              new Object[] { new Integer(0)},
                              new String[] { "org.nosuchpackage.NoSuchClass"});

      Collection c = call.remoteInvoke(dispatcher, 30000);
      assertEquals(1, c.size());
      ServerResponse r = (ServerResponse)c.iterator().next();
      assertEquals(jChannel.getLocalAddress(), r.getAddress());
      assertEquals("nosuchcategory", r.getCategory());
      assertNull(r.getSubordinateID());
      assertTrue(r.getInvocationResult() instanceof ClassNotFoundException);

      jChannel.close();
   }

   public void testNoSuchServer_Local() throws Exception
   {
      Collection result =
            rpcServer.invoke("noSuchCategory", "noSuchMethod", new Object[] {}, new String[] {});
      assertEquals(0, result.size());
   }

   public void testNoSuchServer_Remote() throws Exception
   {
      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());

      RpcServerCall call =
            new RpcServerCall("nosuchcategory",
                              "nosuchmethod",
                              new Object[] {},
                              new String[] {});

      Collection c = call.remoteInvoke(dispatcher, 30000);
      assertEquals(0, c.size());

      jChannel.close();
   }

   public void testNoSuchMethodName_Local() throws Exception
   {
      assertTrue(rpcServer.register("someCategory", new Subordinate("SIMPLE")));

      Collection c =
            rpcServer.invoke("someCategory",
                             "extraMethod",
                             new Object[] { new Integer(1)},
                             new String[] { "java.lang.Integer"});

      assertEquals(1, c.size());
      SubordinateServerResponse r = (SubordinateServerResponse)c.iterator().next();
      assertEquals("SIMPLE", r.getSubServerID());
      assertTrue(r.getInvocationResult() instanceof NoSuchMethodException);

      assertTrue(rpcServer.register("someCategory", new ExtendedSubordinate("EXTENDED")));

      c = rpcServer.invoke("someCategory",
                           "extraMethod",
                           new Object[] { new Integer(1)},
                           new String[] { "java.lang.Integer"});

      assertEquals(2, c.size());
      Iterator i = c.iterator();
      SubordinateServerResponse r1 = (SubordinateServerResponse)i.next();
      SubordinateServerResponse r2 = (SubordinateServerResponse)i.next();

      if ("SIMPLE".equals(r1.getSubServerID()))
      {
         assertTrue(r1.getInvocationResult() instanceof NoSuchMethodException);
         assertEquals("EXTENDED", r2.getSubServerID());
         assertEquals(new Integer(2), r2.getInvocationResult());
      }
      else
      {
         assertEquals("SIMPLE", r2.getSubServerID());
         assertTrue(r2.getInvocationResult() instanceof NoSuchMethodException);
         assertEquals("EXTENDED", r1.getSubServerID());
         assertEquals(new Integer(2), r1.getInvocationResult());
      }
   }

   public void testNoSuchMethodName_Remote() throws Exception
   {
      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());

      assertTrue(rpcServer.register("someCategory", new Subordinate("SIMPLE")));

      RpcServerCall call =
            new RpcServerCall("someCategory",
                              "extraMethod",
                              new Object[] { new Integer(1)},
                              new String[] { "java.lang.Integer"});

      Collection c = call.remoteInvoke(dispatcher, 30000);

      assertEquals(1, c.size());
      ServerResponse r = (ServerResponse)c.iterator().next();
      assertEquals(jChannel.getLocalAddress(), r.getAddress());
      assertEquals("someCategory", r.getCategory());
      assertEquals("SIMPLE", r.getSubordinateID());
      assertTrue(r.getInvocationResult() instanceof NoSuchMethodException);

      assertTrue(rpcServer.register("someCategory", new ExtendedSubordinate("EXTENDED")));

      c = call.remoteInvoke(dispatcher, 30000);

      assertEquals(2, c.size());
      Iterator i = c.iterator();
      ServerResponse r1 = (ServerResponse)i.next();
      ServerResponse r2 = (ServerResponse)i.next();

      assertEquals(jChannel.getLocalAddress(), r1.getAddress());
      assertEquals(jChannel.getLocalAddress(), r2.getAddress());
      assertEquals("someCategory", r1.getCategory());
      assertEquals("someCategory", r2.getCategory());

      if ("SIMPLE".equals(r1.getSubordinateID()))
      {
         assertTrue(r1.getInvocationResult() instanceof NoSuchMethodException);
         assertEquals("EXTENDED", r2.getSubordinateID());
         assertEquals(new Integer(2), r2.getInvocationResult());
      }
      else
      {
         assertEquals("SIMPLE", r2.getSubordinateID());
         assertTrue(r2.getInvocationResult() instanceof NoSuchMethodException);
         assertEquals("EXTENDED", r1.getSubordinateID());
         assertEquals(new Integer(2), r1.getInvocationResult());
      }

      jChannel.close();
   }

   public void testNoSuchMethodSignature_Local() throws Exception
   {
      assertTrue(rpcServer.register("someCategory", new Subordinate("SIMPLE")));

      Collection c =
            rpcServer.invoke("someCategory",
                             "methodWithOneIntegerArgument",
                             new Object[] { new Float(0) },
                             new String[] { "java.lang.Float"});

      assertEquals(1, c.size());
      SubordinateServerResponse r = (SubordinateServerResponse)c.iterator().next();
      assertEquals("SIMPLE", r.getSubServerID());
      assertTrue(r.getInvocationResult() instanceof NoSuchMethodException);
   }

   public void testNoSuchMethodSignature_Remote() throws Exception
   {
      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());

      assertTrue(rpcServer.register("someCategory", new Subordinate("SIMPLE")));

      RpcServerCall call =
            new RpcServerCall("someCategory",
                              "methodWithOneIntegerArgument",
                              new Object[] { new Float(0)},
                              new String[] { "java.lang.Float"});

      Collection c = call.remoteInvoke(dispatcher, 30000);

      assertEquals(1, c.size());
      ServerResponse r = (ServerResponse)c.iterator().next();
      assertEquals(jChannel.getLocalAddress(), r.getAddress());
      assertEquals("someCategory", r.getCategory());
      assertEquals("SIMPLE", r.getSubordinateID());
      assertTrue(r.getInvocationResult() instanceof NoSuchMethodException);

      jChannel.close();
   }



   public void testInvocationOnOneServer_Local() throws Exception
   {
      String id = "ONE";
      String state = "something";
      Subordinate so = new Subordinate(id);

      assertTrue(rpcServer.register("someCategory", so));

      Collection c = rpcServer.invoke("someCategory",
                                      "setStateAndReturn",
                                      new Object[] { state },
                                      new String[] { "java.lang.String" });
      assertEquals(state, so.getState());
      assertEquals(1, c.size());
      SubordinateServerResponse r = (SubordinateServerResponse)c.iterator().next();
      assertEquals("ONE", r.getSubServerID());
      assertEquals(state + "." + id, r.getInvocationResult());
   }

   public void testInvocationOnOneServer_Remote() throws Exception
   {
      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());
      String id = "ONE";
      String state = "something";
      Subordinate so = new Subordinate(id);

      assertTrue(rpcServer.register("someCategory", so));

      RpcServerCall call =
            new RpcServerCall("someCategory",
                              "setStateAndReturn",
                              new Object[] { state },
                              new String[] { "java.lang.String" });

      Collection c = call.remoteInvoke(dispatcher, 30000);

      assertEquals(state, so.getState());
      assertEquals(1, c.size());
      ServerResponse r = (ServerResponse)c.iterator().next();
      assertEquals(jChannel.getLocalAddress(), r.getAddress());
      assertEquals("someCategory", r.getCategory());
      assertEquals("ONE", r.getSubordinateID());
      assertEquals(state + "." + id, r.getInvocationResult());

      jChannel.close();
   }

   public void testInvocationOnEquivalentServers_Local() throws Exception
   {

      String idOne = "ONE", idTwo = "TWO";
      Subordinate soOne = new Subordinate(idOne);
      Subordinate soTwo = new Subordinate(idTwo);

      assertTrue(rpcServer.register("someCategory", soOne));
      assertTrue(rpcServer.register("someCategory", soTwo));

      String state = "something";

      Collection c = rpcServer.invoke("someCategory",
                                      "setStateAndReturn",
                                      new Object[] { state },
                                      new String[] { "java.lang.String" });

      assertEquals(state, soOne.getState());
      assertEquals(state, soTwo.getState());
      assertEquals(2, c.size());
      Iterator i = c.iterator();
      SubordinateServerResponse r1 = (SubordinateServerResponse)i.next();
      SubordinateServerResponse r2 = (SubordinateServerResponse)i.next();

      if ((state + "." + idOne).equals(r1.getInvocationResult()))
      {
         assertEquals(state + "." + idTwo, r2.getInvocationResult());
      }
      else
      {
         assertEquals(state + "." + idOne, r2.getInvocationResult());
         assertEquals(state + "." + idTwo, r1.getInvocationResult());
      }
   }

   public void testInvocationOnEquivalentServers_Remote() throws Exception
   {

      JChannel jChannel = new JChannel(props);
      RpcDispatcher dispatcher = new RpcDispatcher(jChannel, null, null, rpcServer);
      jChannel.connect("testGroup");
      assertTrue(jChannel.isConnected());
      String idOne = "ONE", idTwo = "TWO";
      Subordinate soOne = new Subordinate(idOne);
      Subordinate soTwo = new Subordinate(idTwo);

      assertTrue(rpcServer.register("someCategory", soOne));
      assertTrue(rpcServer.register("someCategory", soTwo));

      String state = "something";

      RpcServerCall call =
            new RpcServerCall("someCategory",
                              "setStateAndReturn",
                              new Object[] { state },
                              new String[] { "java.lang.String" });

      Collection c = call.remoteInvoke(dispatcher, 30000);

      assertEquals(state, soOne.getState());
      assertEquals(state, soTwo.getState());
      assertEquals(2, c.size());
      Iterator i = c.iterator();
      ServerResponse r1 = (ServerResponse)i.next();
      ServerResponse r2 = (ServerResponse)i.next();

      if ((state + "." + idOne).equals(r1.getInvocationResult()))
      {
         assertEquals(state + "." + idTwo, r2.getInvocationResult());
      }
      else
      {
         assertEquals(state + "." + idOne, r2.getInvocationResult());
         assertEquals(state + "." + idTwo, r1.getInvocationResult());
      }

      jChannel.close();
   }



   // Inner classes -------------------------------------------------

   public class Subordinate implements ServerFacade
   {
      private String state = null;
      private String id = null;

      public Subordinate()
      {
         this(null);
      }

      public Subordinate(String id)
      {
         this.id = id;
      }

      public Serializable getID()
      {
         return id;
      }

      public void someMethod()
      {
      }

      public Integer methodWithOneIntegerArgument(Integer i)
      {
         return new Integer(i.intValue() + 1);
      }

      public String setStateAndReturn(String state)
      {
         this.state = state;
         return state + "." + id;
      }

      public String getState()
      {
         return state;
      }

      public String toString()
      {
         return "Subordinate[id=" + id + ", state=" + state + "]";
      }

   }

   public class ExtendedSubordinate extends Subordinate
   {
      public ExtendedSubordinate()
      {
         this(null);
      }

      public ExtendedSubordinate(String id)
      {
         super(id);
      }

      public Integer extraMethod(Integer i)
      {
         return new Integer(i.intValue() + 1);
      }
   }

}
