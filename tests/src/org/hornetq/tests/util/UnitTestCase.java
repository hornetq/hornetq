/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.Context;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import junit.framework.Assert;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.ReferenceDescribe;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.impl.ClusterManagerImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.utils.UUIDGenerator;

/**
 *
 * Helper base class for our unit tests
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert</a>
 *
 */
public class UnitTestCase extends TestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(UnitTestCase.class);

   private static final Logger logInstance = Logger.getLogger(UnitTestCase.class);

   public static final String INVM_ACCEPTOR_FACTORY = "org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory";

   public static final String INVM_CONNECTOR_FACTORY = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory";

   public static final String NETTY_ACCEPTOR_FACTORY = "org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory";

   public static final String NETTY_CONNECTOR_FACTORY = "org.hornetq.core.remoting.impl.netty.NettyConnectorFactory";

   // Attributes ----------------------------------------------------

   private final String baseDir = System.getProperty("java.io.tmpdir", "/tmp") + File.separator + "hornetq-unit-test";

   private long timeStart = System.currentTimeMillis();

   public long getTimeStart()
   {
      return timeStart;
   }

   private String testDir = baseDir + File.separator + timeStart;

   public void setTimeStart(long time)
   {
      timeStart = time;
      testDir = baseDir + File.separator + timeStart;
   }

   // There is a verification about thread leakages. We only fail a single thread when this happens
   private static Set<Thread> alreadyFailedThread = new HashSet<Thread>();

   private boolean checkThread = true;

   protected void disableCheckThread()
   {
      checkThread = false;
   }

   private String osType = System.getProperty("os.name").toLowerCase();

   protected boolean isWindows()
   {
      return (osType.indexOf("win") >= 0);
   }

   // Static --------------------------------------------------------

   protected Configuration createDefaultConfig()
   {
      return createDefaultConfig(false);
   }

   protected Configuration createDefaultConfig(final boolean netty)
   {
      if (netty)
      {
         return createDefaultConfig(new HashMap<String, Object>(), INVM_ACCEPTOR_FACTORY, NETTY_ACCEPTOR_FACTORY);
      }
      else
      {
         return createDefaultConfig(new HashMap<String, Object>(), INVM_ACCEPTOR_FACTORY);
      }
   }

   protected Configuration createClusteredDefaultConfig(final int index,
                                                               final Map<String, Object> params,
                                                               final String... acceptors)
   {
      Configuration config = createDefaultConfig(index, params, acceptors);

      config.setClustered(true);

      return config;
   }

   protected Configuration createDefaultConfig(final int index,
                                               final Map<String, Object> params,
                                               final String... acceptors)
   {
      Configuration configuration = createBasicConfig(index);

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }

      return configuration;
   }

   protected ConfigurationImpl createBasicConfig()
   {
      return createBasicConfig(0);
   }

   /**
    * @param serverID
    * @return
    */
   protected ConfigurationImpl createBasicConfig(final int serverID)
   {
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(serverID, false));
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir(serverID, false));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(getDefaultJournalType());
      configuration.setPagingDirectory(getPageDir(serverID, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(serverID, false));
      configuration.setJournalCompactMinFiles(0);
      configuration.setJournalCompactPercentage(0);
      return configuration;
   }

   protected Configuration createDefaultConfig(final Map<String, Object> params, final String... acceptors)
   {
      Configuration configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJMXManagementEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir());
      configuration.setJournalMinFiles(2);
      configuration.setJournalDirectory(getJournalDir());
      configuration.setJournalFileSize(100 * 1024);
      configuration.setPagingDirectory(getPageDir());
      configuration.setLargeMessagesDirectory(getLargeMessagesDir());
      configuration.setJournalCompactMinFiles(0);
      configuration.setJournalCompactPercentage(0);

      configuration.setFileDeploymentEnabled(false);

      configuration.setJournalType(getDefaultJournalType());

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }

      return configuration;
   }

   protected String getUDPDiscoveryAddress()
   {
      return System.getProperty("TEST-UDP-ADDRESS", "230.1.2.3");
   }

   protected String getUDPDiscoveryAddress(int variant)
   {
      String value = getUDPDiscoveryAddress();

      int posPoint = value.lastIndexOf('.');

      int last = Integer.valueOf(value.substring(posPoint + 1));

      return value.substring(0, posPoint + 1) + (last + variant);
   }

   public int getUDPDiscoveryPort()
   {
      return Integer.parseInt(System.getProperty("TEST-UDP-PORT", "6750"));
   }

   public int getUDPDiscoveryPort(final int variant)
   {
      return getUDPDiscoveryPort() + variant;
   }

   protected static JournalType getDefaultJournalType()
   {
      if (AsynchronousFileImpl.isLoaded())
      {
         return JournalType.ASYNCIO;
      }
      else
      {
         return JournalType.NIO;
      }
   }

   /**
    * @param name
    */
   public UnitTestCase(final String name)
   {
      super(name);
   }

   public UnitTestCase()
   {
      super();
   }

   public static void forceGC()
   {
      logInstance.info("#test forceGC");
      WeakReference<Object> dumbReference = new WeakReference<Object>(new Object());
      // A loop that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
         System.gc();
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
         }
      }
      logInstance.info("#test forceGC Done");
   }

   public static void forceGC(Reference<?> ref, long timeout)
   {
      long waitUntil = System.currentTimeMillis() + timeout;
      // A loop that will wait GC, using the minimal time as possible
      while (ref.get() != null && System.currentTimeMillis() < waitUntil)
      {
         ArrayList<String> list = new ArrayList<String>();
         for (int i = 0; i < 1000; i++)
         {
            list.add("Some string with garbage with concatenation " + i);
         }
         list.clear();
         list = null;
         System.gc();
         try
         {
            Thread.sleep(500);
         }
         catch (InterruptedException e)
         {
         }
      }
   }

   // verify if these weak references are released after a few GCs
   public static void checkWeakReferences(final WeakReference<?>... references)
   {

      int i = 0;
      boolean hasValue = false;

      do
      {
         hasValue = false;

         if (i > 0)
         {
            UnitTestCase.forceGC();
         }

         for (WeakReference<?> ref : references)
         {
            if (ref.get() != null)
            {
               hasValue = true;
            }
         }
      }
      while (i++ <= 30 && hasValue);

      for (WeakReference<?> ref : references)
      {
         Assert.assertNull(ref.get());
      }
   }

   public static String threadDump(final String msg)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println("*******************************************************************************");
      out.println("Complete Thread dump " + msg);

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println("Thread " + el.getKey() +
                     " name = " +
                     el.getKey().getName() +
                     " id = " +
                     el.getKey().getId() +
                     " group = " +
                     el.getKey().getThreadGroup());
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }

      out.println("===============================================================================");
      out.println("End Thread dump " + msg);
      out.println("*******************************************************************************");

      return str.toString();
   }

   /** Sends the message to both logger and System.out (for unit report) */
   public void logAndSystemOut(String message, Exception e)
   {
      Logger log = Logger.getLogger(this.getClass());
      log.info(message, e);
      System.out.println(message);
      e.printStackTrace(System.out);
   }

   /** Sends the message to both logger and System.out (for unit report) */
   public void logAndSystemOut(String message)
   {
      Logger log = Logger.getLogger(this.getClass());
      log.info(message);
      System.out.println(this.getClass().getName() + "::" + message);
   }

   protected static TestSuite createAIOTestSuite(final Class<?> clazz)
   {
      TestSuite suite = new TestSuite(clazz.getName() + " testsuite");

      if (AIOSequentialFileFactory.isSupported())
      {
         suite.addTestSuite(clazz);
      }
      else
      {
         // System.out goes towards JUnit report
         System.out.println("Test " + clazz.getName() + " ignored as AIO is not available");
      }

      return suite;
   }

   public static String dumpBytes(final byte[] bytes)
   {
      StringBuffer buff = new StringBuffer();

      buff.append(System.identityHashCode(bytes) + ", size: " + bytes.length + " [");

      for (int i = 0; i < bytes.length; i++)
      {
         buff.append(bytes[i]);

         if (i != bytes.length - 1)
         {
            buff.append(", ");
         }
      }

      buff.append("]");

      return buff.toString();
   }

   public static String dumbBytesHex(final byte[] buffer, final int bytesPerLine)
   {

      StringBuffer buff = new StringBuffer();

      buff.append("[");

      for (int i = 0; i < buffer.length; i++)
      {
         buff.append(String.format("%1$2X", buffer[i]));
         if (i + 1 < buffer.length)
         {
            buff.append(", ");
         }
         if ((i + 1) % bytesPerLine == 0)
         {
            buff.append("\n ");
         }
      }
      buff.append("]");

      return buff.toString();
   }

   public static void assertEqualsByteArrays(final byte[] expected, final byte[] actual)
   {
      // assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertEqualsTransportConfigurations(final TransportConfiguration[] expected,
                                                          final TransportConfiguration[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         Assert.assertEquals("TransportConfiguration at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertEqualsBuffers(final int size, final HornetQBuffer expected, final HornetQBuffer actual)
   {
      // assertEquals(expected.length, actual.length);
      expected.readerIndex(0);
      actual.readerIndex(0);

      for (int i = 0; i < size; i++)
      {
         byte b1 = expected.readByte();
         byte b2 = actual.readByte();
         Assert.assertEquals("byte at index " + i, b1, b2);
      }
      expected.resetReaderIndex();
      actual.resetReaderIndex();
   }

   public static void assertEqualsByteArrays(final int length, final byte[] expected, final byte[] actual)
   {
      // we check only for the given length (the arrays might be
      // larger)
      Assert.assertTrue(expected.length >= length);
      Assert.assertTrue(actual.length >= length);
      for (int i = 0; i < length; i++)
      {
         Assert.assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertSameXids(final List<Xid> expected, final List<Xid> actual)
   {
      Assert.assertNotNull(expected);
      Assert.assertNotNull(actual);
      Assert.assertEquals(expected.size(), actual.size());

      for (int i = 0; i < expected.size(); i++)
      {
         Xid expectedXid = expected.get(i);
         Xid actualXid = actual.get(i);
         UnitTestCase.assertEqualsByteArrays(expectedXid.getBranchQualifier(), actualXid.getBranchQualifier());
         Assert.assertEquals(expectedXid.getFormatId(), actualXid.getFormatId());
         UnitTestCase.assertEqualsByteArrays(expectedXid.getGlobalTransactionId(), actualXid.getGlobalTransactionId());
      }
   }

   protected static void checkNoBinding(final Context context, final String binding)
   {
      try
      {
         context.lookup(binding);
         Assert.fail("there must be no resource to look up for " + binding);
      }
      catch (Exception e)
      {
      }
   }

   protected static Object checkBinding(final Context context, final String binding) throws Exception
   {
      Object o = context.lookup(binding);
      Assert.assertNotNull(o);
      return o;
   }

   /**
    * @param connectorConfigs
    * @return
    */
   protected ArrayList<String> registerConnectors(final HornetQServer server,
                                                  final List<TransportConfiguration> connectorConfigs)
   {
      // The connectors need to be pre-configured at main config object but this method is taking
      // TransportConfigurations directly
      // So this will first register them at the config and then generate a list of objects
      ArrayList<String> connectors = new ArrayList<String>();
      for (TransportConfiguration tnsp : connectorConfigs)
      {
         String name = RandomUtil.randomString();

         server.getConfiguration().getConnectorConfigurations().put(name, tnsp);

         connectors.add(name);
      }
      return connectors;
   }

   protected static void checkFreePort(final int... ports)
   {
      for (int port : ports)
      {
         ServerSocket ssocket = null;
         try
         {
            ssocket = new ServerSocket(port);
         }
         catch (Exception e)
         {
            throw new IllegalStateException("port " + port + " is bound");
         }
         finally
         {
            if (ssocket != null)
            {
               try
               {
                  ssocket.close();
               }
               catch (IOException e)
               {
               }
            }
         }
      }
   }
   
   public static void crashAndWaitForFailure(HornetQServer server, ClientSession ...sessions) throws Exception
   {
      CountDownLatch latch = new CountDownLatch(sessions.length);
      for (ClientSession session : sessions)
      {
         CountDownSessionFailureListener listener = new CountDownSessionFailureListener(latch);
         session.addFailureListener(listener);
      }

      ClusterManager clusterManager = server.getClusterManager();
      clusterManager.flushExecutor();
      ((ClusterManagerImpl)clusterManager).clear();
      Assert.assertTrue("server should be running!", server.isStarted());
      server.stop(true);

      if (sessions.length > 0)
      {
         // Wait to be informed of failure
         boolean ok = latch.await(10000, TimeUnit.MILLISECONDS);
         Assert.assertTrue("Failed to stop the server! Latch count is " + latch.getCount() + " out of " +
                  sessions.length, ok);
      }
   }

  public static void crashAndWaitForFailure(HornetQServer server, ServerLocator locator) throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession();
      try
      {
         crashAndWaitForFailure(server, session);
      }
      finally
      {
         try
         {
            session.close();
            sf.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   // Constructors --------------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @return the testDir
    */
   protected String getTestDir()
   {
      return testDir;
   }

   protected void clearData()
   {
      clearData(getTestDir());
   }

   protected void clearData(final String testDir)
   {
      // Need to delete the root

      File file = new File(baseDir);
      deleteDirectory(file);
      file.mkdirs();

      recreateDirectory(getJournalDir(testDir));
      recreateDirectory(getBindingsDir(testDir));
      recreateDirectory(getPageDir(testDir));
      recreateDirectory(getLargeMessagesDir(testDir));
      recreateDirectory(getClientLargeMessagesDir(testDir));
      recreateDirectory(getTemporaryDir(testDir));
   }

   /**
    * @return the journalDir
    */
   public String getJournalDir()
   {
      return getJournalDir(getTestDir());
   }

   protected String getJournalDir(final String testDir)
   {
      return testDir + "/journal";
   }

   protected String getJournalDir(final int index, final boolean backup)
   {
      String dir = getJournalDir(getTestDir()) + index + "-" + (backup ? "B" : "L");

      return dir;
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir()
   {
      return getBindingsDir(getTestDir());
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(final String testDir)
   {
      return testDir + "/bindings";
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(final int index, final boolean backup)
   {
      return getBindingsDir(getTestDir()) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the pageDir
    */
   protected String getPageDir()
   {
      return getPageDir(getTestDir());
   }

   /**
    * @return the pageDir
    */
   protected String getPageDir(final String testDir)
   {
      return testDir + "/page";
   }

   protected String getPageDir(final int index, final boolean backup)
   {
      return getPageDir(getTestDir()) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the largeMessagesDir
    */
   protected String getLargeMessagesDir()
   {
      return getLargeMessagesDir(getTestDir());
   }

   /**
    * @return the largeMessagesDir
    */
   protected String getLargeMessagesDir(final String testDir)
   {
      return testDir + "/large-msg";
   }

   protected String getLargeMessagesDir(final int index, final boolean backup)
   {
      return getLargeMessagesDir(getTestDir()) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir()
   {
      return getClientLargeMessagesDir(getTestDir());
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir(final String testDir)
   {
      return testDir + "/client-large-msg";
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir()
   {
      return getTemporaryDir(getTestDir());
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir(final String testDir)
   {
      return testDir + "/temp";
   }

   protected static void expectHornetQException(final String message, final int errorCode, final HornetQAction action)
   {
      try
      {
         action.run();
         Assert.fail(message);
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof HornetQException);
         Assert.assertEquals(errorCode, ((HornetQException)e).getCode());
      }
   }

   protected static void expectHornetQException(final int errorCode, final HornetQAction action)
   {
      UnitTestCase.expectHornetQException("must throw a HornetQException with the expected errorCode: " + errorCode,
                                          errorCode,
                                          action);
   }

   protected static void expectXAException(final int errorCode, final HornetQAction action)
   {
      try
      {
         action.run();
         Assert.fail("must throw a XAException with the expected errorCode: " + errorCode);
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof XAException);
         Assert.assertEquals(errorCode, ((XAException)e).errorCode);
      }
   }

   public static byte getSamplebyte(final long position)
   {
      return (byte)('a' + position % ('z' - 'a' + 1));
   }

   // Creates a Fake LargeStream without using a real file
   public static InputStream createFakeLargeStream(final long size) throws Exception
   {
      return new InputStream()
      {
         private long count;

         private boolean closed = false;

         @Override
         public void close() throws IOException
         {
            super.close();
            closed = true;
         }

         @Override
         public int read() throws IOException
         {
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
            if (count++ < size)
            {
               return UnitTestCase.getSamplebyte(count - 1);
            }
            else
            {
               return -1;
            }
         }
      };

   }

   /** It validates a Bean (POJO) using simple setters and getters with random values.
    *  You can pass a list of properties to be ignored, as some properties will have a pre-defined domain (not being possible to use random-values on them) */
   protected void validateGettersAndSetters(final Object pojo, final String... ignoredProperties) throws Exception
   {
      HashSet<String> ignoreSet = new HashSet<String>();

      for (String ignore : ignoredProperties)
      {
         ignoreSet.add(ignore);
      }

      BeanInfo info = Introspector.getBeanInfo(pojo.getClass());

      PropertyDescriptor properties[] = info.getPropertyDescriptors();

      for (PropertyDescriptor prop : properties)
      {
         Object value;

         if (prop.getPropertyType() == String.class)
         {
            value = RandomUtil.randomString();
         }
         else if (prop.getPropertyType() == Integer.class || prop.getPropertyType() == Integer.TYPE)
         {
            value = RandomUtil.randomInt();
         }
         else if (prop.getPropertyType() == Long.class || prop.getPropertyType() == Long.TYPE)
         {
            value = RandomUtil.randomLong();
         }
         else if (prop.getPropertyType() == Boolean.class || prop.getPropertyType() == Boolean.TYPE)
         {
            value = RandomUtil.randomBoolean();
         }
         else if (prop.getPropertyType() == Double.class || prop.getPropertyType() == Double.TYPE)
         {
            value = RandomUtil.randomDouble();
         }
         else
         {
            System.out.println("Can't validate property of type " + prop.getPropertyType() + " on " + prop.getName());
            value = null;
         }

         if (value != null && prop.getWriteMethod() != null && prop.getReadMethod() == null)
         {
            System.out.println("WriteOnly property " + prop.getName() + " on " + pojo.getClass());
         }
         else if (value != null & prop.getWriteMethod() != null &&
                  prop.getReadMethod() != null &&
                  !ignoreSet.contains(prop.getName()))
         {
            System.out.println("Validating " + prop.getName() + " type = " + prop.getPropertyType());
            prop.getWriteMethod().invoke(pojo, value);

            Assert.assertEquals("Property " + prop.getName(), value, prop.getReadMethod().invoke(pojo));
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   Map<Thread, StackTraceElement[]> previousThreads;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      OperationContextImpl.clearContext();

      clearData(getTestDir());

      InVMRegistry.instance.clear();

      // checkFreePort(TransportConstants.DEFAULT_PORT);

      previousThreads = Thread.getAllStackTraces();

      logAndSystemOut("#test " + getName());
   }

   @Override
   protected void tearDown() throws Exception
   {
      cleanupPools();

      deleteDirectory(new File(baseDir));


      Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
      for (Thread thread : threadMap.keySet())
      {
         StackTraceElement[] stack = threadMap.get(thread);
         for (StackTraceElement stackTraceElement : stack)
         {
            if (stackTraceElement.getMethodName().contains("getConnectionWithRetry") && !alreadyFailedThread.contains(thread))
            {
               alreadyFailedThread.add(thread);
               System.out.println(threadDump(this.getName() + " has left threads running. Look at thread " +
                                             thread.getName() +
                                             " id = " +
                                             thread.getId() +
                                             " has running locators on test " +
                                             this.getName() +
                                             " on this following dump"));
               fail("test left serverlocator running, this could effect other tests");
            }
            else if (stackTraceElement.getMethodName().contains("BroadcastGroupImpl.run") && !alreadyFailedThread.contains(thread))
            {
               alreadyFailedThread.add(thread);
               System.out.println(threadDump(this.getName() + " has left threads running. Look at thread " +
                                             thread.getName() +
                                             " id = " +
                                             thread.getId() +
                                             " is still broadcasting " +
                                             this.getName() +
                                             " on this following dump"));
               fail("test left broadcastgroupimpl running, this could effect other tests");
            }
         }
      }

       if (checkThread)
      {
          StringBuffer buffer = null;

          boolean failed = true;


         long timeout = System.currentTimeMillis() + 60000;
         while (failed && timeout > System.currentTimeMillis())
         {
            buffer = new StringBuffer();

            failed = checkThread(buffer);

            if (failed)
            {
               forceGC();
               Thread.sleep(500);
               log.info("There are still threads running, trying again");
            }
         }

         if (failed)
         {
            logAndSystemOut("Thread leaked on test " + this.getClass().getName() +
                            "::" +
                            this.getName() +
                            "\n" +
                            buffer.toString());
            logAndSystemOut("Thread leakage");

            fail("Thread leaked");
         }

      }
      else
      {
         checkThread = true;
      }


      super.tearDown();
   }

   /**
    * @param buffer
    * @return
    */
   private boolean checkThread(StringBuffer buffer)
   {
      boolean failedThread = false;

      Map<Thread, StackTraceElement[]> postThreads = Thread.getAllStackTraces();

      if (postThreads.size() > previousThreads.size())
      {

         buffer.append("*********************************************************************************\n");
         buffer.append("LEAKING THREADS\n");

         for (Thread aliveThread : postThreads.keySet())
         {
            final String name = aliveThread.getName();
            final boolean notSunPKCS11 = !name.contains("SunPKCS11");
            final boolean notAttachListener = !name.contains("Attach Listener");
            ThreadGroup group = aliveThread.getThreadGroup();
            final boolean isSystemThread = group != null && "system".equals(group.getName());
            // 'process reaper' is a normal JVM thread that will run when we call Runtime.exec()
            final boolean notProcessReaper = isSystemThread && !name.equals("process reaper");
            if (notSunPKCS11 && notAttachListener && notProcessReaper && !previousThreads.containsKey(aliveThread))
            {
               failedThread = true;
               buffer.append("=============================================================================\n");
               buffer.append("Thread " + aliveThread + " is still alive with the following stackTrace:\n");
               StackTraceElement[] elements = postThreads.get(aliveThread);
               for (StackTraceElement el : elements)
               {
                  buffer.append(el + "\n");
               }
            }

         }
         buffer.append("*********************************************************************************\n");

      }
      return failedThread;
   }

   /**
    *
    */
   protected void cleanupPools() throws Exception
   {
      OperationContextImpl.clearContext();
      
      long timeout = System.currentTimeMillis() + 5000;

      while (InVMRegistry.instance.size() > 0  && timeout > System.currentTimeMillis())
      {
         Thread.sleep(100);
      }

      if (InVMRegistry.instance.size() > 0)
      {
         InVMRegistry.instance.clear();
         log.info(threadDump("Thread dump"));
         Assert.assertEquals(0, InVMRegistry.instance.size());

      }

      timeout = System.currentTimeMillis() + 15000;

      while (AsynchronousFileImpl.getTotalMaxIO() != 0 && System.currentTimeMillis() > timeout)
      {
         try
         {
            Thread.sleep(500);
         }
         catch (Exception ignored)
         {
         }
      }

      if (AsynchronousFileImpl.getTotalMaxIO() != 0)
      {
         AsynchronousFileImpl.resetMaxAIO();
         Assert.fail("test did not close all its files " + AsynchronousFileImpl.getTotalMaxIO());
      }

      // We shutdown the global pools to give a better isolation between tests
      try
      {
         ServerLocatorImpl.clearThreadPools();
      }
      catch (Throwable e)
      {
         log.info(threadDump(e.getMessage()));
         System.err.println(threadDump(e.getMessage()));
      }
   }

   protected byte[] autoEncode(final Object... args)
   {

      int size = 0;

      for (Object arg : args)
      {
         if (arg instanceof Byte)
         {
            size++;
         }
         else if (arg instanceof Boolean)
         {
            size++;
         }
         else if (arg instanceof Integer)
         {
            size += 4;
         }
         else if (arg instanceof Long)
         {
            size += 8;
         }
         else if (arg instanceof Float)
         {
            size += 4;
         }
         else if (arg instanceof Double)
         {
            size += 8;
         }
         else
         {
            throw new IllegalArgumentException("method autoEncode doesn't know how to convert " + arg.getClass() +
                                               " yet");
         }
      }

      ByteBuffer buffer = ByteBuffer.allocate(size);

      for (Object arg : args)
      {
         if (arg instanceof Byte)
         {
            buffer.put(((Byte)arg).byteValue());
         }
         else if (arg instanceof Boolean)
         {
            Boolean b = (Boolean)arg;
            buffer.put((byte)(b.booleanValue() ? 1 : 0));
         }
         else if (arg instanceof Integer)
         {
            buffer.putInt(((Integer)arg).intValue());
         }
         else if (arg instanceof Long)
         {
            buffer.putLong(((Long)arg).longValue());
         }
         else if (arg instanceof Float)
         {
            buffer.putFloat(((Float)arg).floatValue());
         }
         else if (arg instanceof Double)
         {
            buffer.putDouble(((Double)arg).doubleValue());
         }
         else
         {
            throw new IllegalArgumentException("method autoEncode doesn't know how to convert " + arg.getClass() +
                                               " yet");
         }
      }

      return buffer.array();
   }

   protected void recreateDirectory(final String directory)
   {
      File file = new File(directory);
      deleteDirectory(file);
      file.mkdirs();
   }

   protected boolean deleteDirectory(final File directory)
   {
      if (!directory.exists())
      {
         return true;
      }
      else
      if (directory.isDirectory())
      {
         String[] files = directory.list();

         for (int j = 0; j < files.length; j++)
         {
            try
            {

               File fileTmp = new File(directory, files[j]);
               if (!deleteDirectory(fileTmp))
               {
                  // This is because of Windows is dumb on releasing files
                  log.warn("could not delete " + fileTmp);
                  forceGC();
                  if (!deleteDirectory(fileTmp))
                  {
                     log.warn("**************************************************************");
                     log.warn("could not delete " + fileTmp + " even afer a retry on GC");
                     log.warn("**************************************************************");
                  }
                  else
                  {
                     log.info(fileTmp + " was deleted without a problem after a retry on GC");
                  }
                  return false;
               }
            }
            catch (Throwable e)
            {
               e.printStackTrace();
            }
         }
      }

      return directory.delete();
   }

   protected void copyRecursive(final File from, final File to) throws Exception
   {
      if (from.isDirectory())
      {
         if (!to.exists())
         {
            to.mkdir();
         }

         String[] subs = from.list();

         for (String sub : subs)
         {
            copyRecursive(new File(from, sub), new File(to, sub));
         }
      }
      else
      {
         InputStream in = null;

         OutputStream out = null;

         try
         {
            in = new BufferedInputStream(new FileInputStream(from));

            out = new BufferedOutputStream(new FileOutputStream(to));

            int b;

            while ((b = in.read()) != -1)
            {
               out.write(b);
            }
         }
         finally
         {
            if (in != null)
            {
               in.close();
            }

            if (out != null)
            {
               out.close();
            }
         }
      }
   }

   protected void assertRefListsIdenticalRefs(final List<MessageReference> l1, final List<MessageReference> l2)
   {
      if (l1.size() != l2.size())
      {
         Assert.fail("Lists different sizes: " + l1.size() + ", " + l2.size());
      }

      Iterator<MessageReference> iter1 = l1.iterator();
      Iterator<MessageReference> iter2 = l2.iterator();

      while (iter1.hasNext())
      {
         MessageReference o1 = iter1.next();
         MessageReference o2 = iter2.next();

         Assert.assertTrue("expected " + o1 + " but was " + o2, o1 == o2);
      }
   }

   protected ServerMessage generateMessage(final long id)
   {
      ServerMessage message = new ServerMessageImpl(id, 1000);

      message.setMessageID(id);

      message.getBodyBuffer().writeString(UUID.randomUUID().toString());

      message.setAddress(new SimpleString("foo"));

      return message;
   }

   protected MessageReference generateReference(final Queue queue, final long id)
   {
      ServerMessage message = generateMessage(id);

      return message.createReference(queue);
   }

   protected int calculateRecordSize(final int size, final int alignment)
   {
      return (size / alignment + (size % alignment != 0 ? 1 : 0)) * alignment;
   }

   protected ClientMessage createTextMessage(final ClientSession session, final String s)
   {
      return createTextMessage(session, s, true);
   }


   protected ClientMessage createTextMessage(final ClientSession session, final String s, final boolean durable)
   {
      ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                    durable,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    (byte)4);
      message.getBodyBuffer().writeString(s);
      return message;
   }

   protected XidImpl newXID()
   {
      return new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
   }

   protected int getMessageCount(final HornetQServer service, final String address) throws Exception
   {
      return getMessageCount(service.getPostOffice(), address);
   }

   /**
    * @param address
    * @param postOffice
    * @return
    * @throws Exception
    */
   protected int getMessageCount(final PostOffice postOffice, final String address) throws Exception
   {
      int messageCount = 0;

      List<QueueBinding> bindings = getLocalQueueBindings(postOffice, address);

      for (QueueBinding qBinding : bindings)
      {
         messageCount += qBinding.getQueue().getMessageCount();
      }

      return messageCount;
   }

   protected List<QueueBinding> getLocalQueueBindings(final PostOffice postOffice, final String address) throws Exception
   {
      ArrayList<QueueBinding> bindingsFound = new ArrayList<QueueBinding>();

      Bindings bindings = postOffice.getBindingsForAddress(new SimpleString(address));

      for (Binding binding : bindings.getBindings())
      {
         if (binding instanceof LocalQueueBinding)
         {
            bindingsFound.add((QueueBinding)binding);
         }
      }
      return bindingsFound;
   }

   /**
    * It will inspect the journal directly and determine if there are queues on this journal,
    * @return a Map containing the reference counts per queue
    * @param serverToInvestigate
    * @throws Exception
    */
   protected Map<Long, AtomicInteger> loadQueues(HornetQServer serverToInvestigate) throws Exception
   {
      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(serverToInvestigate.getConfiguration()
                                                                                         .getJournalDirectory());

      JournalImpl messagesJournal = new JournalImpl(serverToInvestigate.getConfiguration().getJournalFileSize(),
                                                    serverToInvestigate.getConfiguration().getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "hornetq-data",
                                                    "hq",
                                                    1);
      List<RecordInfo> records = new LinkedList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

      messagesJournal.start();
      messagesJournal.load(records, preparedTransactions, null);

      // These are more immutable integers
      Map<Long, AtomicInteger> messageRefCounts = new HashMap<Long, AtomicInteger>();

      for (RecordInfo info : records)
      {
         Object o = JournalStorageManager.newObjectEncoding(info);
         if (info.getUserRecordType() == JournalStorageManager.ADD_REF)
         {
            ReferenceDescribe ref = (ReferenceDescribe)o;
            AtomicInteger count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null)
            {
               count = new AtomicInteger(1);
               messageRefCounts.put(ref.refEncoding.queueID, count);
            }
            else
            {
               count.incrementAndGet();
            }
         }
      }

      messagesJournal.stop();

      return messageRefCounts;

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected static interface HornetQAction
   {
      void run() throws Exception;
   }

}
