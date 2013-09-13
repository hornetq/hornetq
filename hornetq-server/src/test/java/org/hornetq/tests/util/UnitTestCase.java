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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.Context;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.persistence.impl.journal.DescribeJournal;
import org.hornetq.core.persistence.impl.journal.DescribeJournal.ReferenceDescribe;
import org.hornetq.core.persistence.impl.journal.JournalRecordIds;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.CoreUnitTestCase;
import org.hornetq.utils.UUIDGenerator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * Helper base class for our unit tests.
 * <p>
 * See {@code org.hornetq.tests.util.ServiceTestBase} for a test case with server set-up.
 * @see ServiceTestBase
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert</a>
 */
public abstract class UnitTestCase extends CoreUnitTestCase
{
   // Constants -----------------------------------------------------

   @Rule
   public TestName name = new TestName();

   @Rule
   public TemporaryFolder temporaryFolder = new TemporaryFolder();
   private String testDir;

   private static final HornetQServerLogger log = HornetQServerLogger.LOGGER;

   public static final String INVM_ACCEPTOR_FACTORY = InVMAcceptorFactory.class.getCanonicalName();

   public static final String INVM_CONNECTOR_FACTORY = InVMConnectorFactory.class.getCanonicalName();

   public static final String NETTY_ACCEPTOR_FACTORY = NettyAcceptorFactory.class.getCanonicalName();

   public static final String NETTY_CONNECTOR_FACTORY = NettyConnectorFactory.class.getCanonicalName();

   protected static final String CLUSTER_PASSWORD = "UnitTestsClusterPassword";

   private static final String OS_TYPE = System.getProperty("os.name").toLowerCase();
   private static final int DEFAULT_UDP_PORT;
   static
   {
      Random random = new Random();
      DEFAULT_UDP_PORT = 6000 + random.nextInt(1000);
   }

   // Attributes ----------------------------------------------------

   // There is a verification about thread leakages. We only fail a single thread when this happens
   private static Set<Thread> alreadyFailedThread = new HashSet<Thread>();

   private final Collection<HornetQServer> servers = new ArrayList<HornetQServer>();
   private final Collection<ServerLocator> locators = new ArrayList<ServerLocator>();
   private final Collection<ClientSessionFactory> sessionFactories = new ArrayList<ClientSessionFactory>();
   private final Collection<ClientSession> clientSessions = new HashSet<ClientSession>();
   private final Collection<ClientConsumer> clientConsumers = new HashSet<ClientConsumer>();
   private final Collection<ClientProducer> clientProducers = new HashSet<ClientProducer>();
   private final Collection<HornetQComponent> otherComponents = new HashSet<HornetQComponent>();

   private boolean checkThread = true;

   protected void disableCheckThread()
   {
      checkThread = false;
   }

   protected String getName()
   {
      return name.getMethodName();
   }

   protected boolean isWindows()
   {
      return (OS_TYPE.indexOf("win") >= 0);
   }

   // Static --------------------------------------------------------

   protected Configuration createDefaultConfig() throws Exception
   {
      return createDefaultConfig(false);
   }

   protected Configuration createDefaultConfig(final boolean netty) throws Exception
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

   protected static final void basicClusterConnectionConfig(Configuration mainConfig, String connectorName,
                                                            String... connectors)
   {
      ArrayList<String> connectors0 = new ArrayList<String>();
      for (String c : connectors)
      {
         connectors0.add(c);
      }
      basicClusterConnectionConfig(mainConfig, connectorName, connectors0);
   }

   protected static final void basicClusterConnectionConfig(Configuration mainConfig,
                                                                                      String connectorName,
                                                                                      List<String> connectors)
   {
      ClusterConnectionConfiguration ccc =
               new ClusterConnectionConfiguration("cluster1", "jms", connectorName, 10, false, false, 1, 1, connectors,
                                                  false);
      mainConfig.getClusterConfigurations().add(ccc);
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

   protected ConfigurationImpl createBasicConfig() throws Exception
   {
      return createBasicConfig(0);
   }

   /**
    * @param serverID
    * @return
    * @throws Exception
    */
   protected final ConfigurationImpl createBasicConfig(final int serverID)
   {
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setJournalFileSize(100 * 1024);

      configuration.setJournalType(getDefaultJournalType());

      configuration.setJournalDirectory(getJournalDir(serverID, false));
      configuration.setBindingsDirectory(getBindingsDir(serverID, false));
      configuration.setPagingDirectory(getPageDir(serverID, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(serverID, false));

      configuration.setJournalCompactMinFiles(0);
      configuration.setJournalCompactPercentage(0);
      configuration.setClusterPassword(CLUSTER_PASSWORD);
      return configuration;
   }

   protected Configuration
            createDefaultConfig(final Map<String, Object> params, final String... acceptors) throws Exception
   {
      ConfigurationImpl configuration = createBasicConfig(-1);

      configuration.setFileDeploymentEnabled(false);
      configuration.setJMXManagementEnabled(false);

      configuration.getAcceptorConfigurations().clear();

      for (String acceptor : acceptors)
      {
         TransportConfiguration transportConfig = new TransportConfiguration(acceptor, params);
         configuration.getAcceptorConfigurations().add(transportConfig);
      }
      return configuration;
   }

   protected static String getUDPDiscoveryAddress()
   {
      return System.getProperty("TEST-UDP-ADDRESS", "230.1.2.3");
   }

   protected static String getUDPDiscoveryAddress(final int variant)
   {
      String value = getUDPDiscoveryAddress();

      int posPoint = value.lastIndexOf('.');

      int last = Integer.valueOf(value.substring(posPoint + 1));

      return value.substring(0, posPoint + 1) + (last + variant);
   }

   public static int getUDPDiscoveryPort()
   {
      String port = System.getProperty("TEST-UDP-PORT");
      if (port != null)
      {
         return Integer.parseInt(port);
      }
      return DEFAULT_UDP_PORT;
   }

   public static int getUDPDiscoveryPort(final int variant)
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
      log.info("#test forceGC");
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
      log.info("#test forceGC Done");
   }

   public static void forceGC(final Reference<?> ref, final long timeout)
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

   /**
    * Verifies whether weak references are released after a few GCs.
    * @param references
    * @throws InterruptedException
    */
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
               break;
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
      HornetQServerLogger log0 = HornetQServerLogger.LOGGER;
      log0.info(message, e);
      System.out.println(message);
      e.printStackTrace(System.out);
   }

   /** Sends the message to both logger and System.out (for unit report) */
   public void logAndSystemOut(String message)
   {
      HornetQServerLogger log0 = HornetQServerLogger.LOGGER;
      log0.info(message);
      System.out.println(this.getClass().getName() + "::" + message);
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
         String name1 = RandomUtil.randomString();

         server.getConfiguration().getConnectorConfigurations().put(name1, tnsp);

         connectors.add(name1);
      }
      return connectors;
   }

   protected final static void checkFreePort(final int... ports)
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
            throw new IllegalStateException("port " + port + " is bound", e);
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

   /**
    * @return the testDir
    */
   protected final String getTestDir()
   {
      return testDir;
   }

   protected final void setTestDir(String testDir)
   {
      this.testDir = testDir;
   }

   protected final void clearDataRecreateServerDirs()
   {
      clearDataRecreateServerDirs(getTestDir());
   }

   private final void deleteTmpDir()
   {
      File file = new File(getTestDir());
      deleteDirectory(file);
   }

   protected void clearDataRecreateServerDirs(final String testDir1)
   {
      // Need to delete the root

      File file = new File(testDir1);
      deleteDirectory(file);
      file.mkdirs();

      recreateDirectory(getJournalDir(testDir1));
      recreateDirectory(getBindingsDir(testDir1));
      recreateDirectory(getPageDir(testDir1));
      recreateDirectory(getLargeMessagesDir(testDir1));
      recreateDirectory(getClientLargeMessagesDir(testDir1));
      recreateDirectory(getTemporaryDir(testDir1));
   }

   /**
    * @return the journalDir
    */
   public String getJournalDir()
   {
      return getJournalDir(getTestDir());
   }

   protected String getJournalDir(final String testDir1)
   {
      return testDir1 + "/journal";
   }

   protected String getJournalDir(final int index, final boolean backup)
   {
      return getJournalDir(getTestDir()) + directoryNameSuffix(index, backup);
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
   protected static String getBindingsDir(final String testDir1)
   {
      return testDir1 + "/bindings";
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(final int index, final boolean backup)
   {
      return getBindingsDir(getTestDir()) + directoryNameSuffix(index, backup);
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
   protected static String getPageDir(final String testDir1)
   {
      return testDir1 + "/page";
   }

   protected String getPageDir(final int index, final boolean backup)
   {
      return getPageDir(getTestDir()) + directoryNameSuffix(index, backup);
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
   protected static String getLargeMessagesDir(final String testDir1)
   {
      return testDir1 + "/large-msg";
   }

   protected String getLargeMessagesDir(final int index, final boolean backup)
   {
      return getLargeMessagesDir(getTestDir()) + directoryNameSuffix(index, backup);
   }

   private static String directoryNameSuffix(int index, boolean backup)
   {
      if (index == -1)
         return "";
      return index + "-" + (backup ? "B" : "L");
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
   protected String getClientLargeMessagesDir(final String testDir1)
   {
      return testDir1 + "/client-large-msg";
   }

   /**
    * @return the temporaryDir
    */
   protected final String getTemporaryDir()
   {
      return getTemporaryDir(getTestDir());
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir(final String testDir1)
   {
      return testDir1 + "/temp";
   }

   protected static void expectHornetQException(final String message, final HornetQExceptionType errorCode, final HornetQAction action)
   {
      try
      {
         action.run();
         Assert.fail(message);
      }
      catch (Exception e)
      {
         Assert.assertTrue(e instanceof HornetQException);
         Assert.assertEquals(errorCode, ((HornetQException)e).getType());
      }
   }

   protected static void expectHornetQException(final HornetQExceptionType errorCode, final HornetQAction action)
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

   @Before
   public void setUp() throws Exception
   {
      testDir = temporaryFolder.getRoot().getAbsolutePath();
      clearDataRecreateServerDirs();
      OperationContextImpl.clearContext();

      InVMRegistry.instance.clear();

      // checkFreePort(TransportConstants.DEFAULT_PORT);

      previousThreads = Thread.getAllStackTraces();

      logAndSystemOut("#test " + getName());
   }

   @After
   public void tearDown() throws Exception
   {
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();

      try
      {
         assertAllClientConsumersAreClosed();
         assertAllClientProducersAreClosed();
         assertAllClientSessionsAreClosed();
      }
      finally
      {
         synchronized (servers)
         {
            for (HornetQServer server : servers)
            {
               if (server == null)
                  continue;
               try
               {
                  final ClusterManager clusterManager = server.getClusterManager();
                  if (clusterManager != null)
                  {
                     for (ClusterConnection cc : clusterManager.getClusterConnections())
                     {
                        stopComponent(cc);
                     }
                  }
               }
               catch (Exception e)
               {
                  // no-op
               }
               stopComponentOutputExceptions(server);
            }
            servers.clear();
         }

         closeAllOtherComponents();

         ArrayList<Exception> exceptions;
         try
         {
            exceptions = checkCsfStopped();
         }
         finally
         {
            cleanupPools();
         }
      //clean up pools before failing
      if(!exceptions.isEmpty())
      {
         for (Exception exception : exceptions)
         {
            exception.printStackTrace();
         }
            fail("Client Session Factories still trying to reconnect, see above to see where created");
      }
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
               fail("test '" + getName() + "' left serverlocator running, this could effect other tests");
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
               logAndSystemOut("Thread leaked on test " + this.getClass().getName() + "::" + this.getName() + "\n" +
                        buffer);
               logAndSystemOut("Thread leakage");

            fail("Thread leaked");
         }

      }
      else
      {
         checkThread = true;
      }

         checkFilesUsage();
      }
   }

   private ArrayList<Exception> checkCsfStopped()
   {
      long time = System.currentTimeMillis();
      long waitUntil = time + 5000;
      while(!ClientSessionFactoryImpl.CLOSE_RUNNABLES.isEmpty() && time < waitUntil)
      {
         try
         {
            Thread.sleep(50);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         time = System.currentTimeMillis();
      }
      List<ClientSessionFactoryImpl.CloseRunnable> closeRunnables =
               new ArrayList<ClientSessionFactoryImpl.CloseRunnable>(ClientSessionFactoryImpl.CLOSE_RUNNABLES);
      ArrayList<Exception> exceptions = new ArrayList<Exception>();

      if (!closeRunnables.isEmpty())
      {
         for (ClientSessionFactoryImpl.CloseRunnable closeRunnable : closeRunnables)
         {
            if (closeRunnable != null)
            {
               exceptions.add(closeRunnable.stop().e);
            }
         }
      }

      return exceptions;
   }

   private void assertAllClientProducersAreClosed()
   {
      synchronized (clientProducers)
      {
         for (ClientProducer p : clientProducers)
         {
               assertTrue(p + " should be closed", p.isClosed());
         }
         clientProducers.clear();
      }
   }

   /**
    *
    */
   private void closeAllOtherComponents()
   {
      synchronized (otherComponents)
      {
         for (HornetQComponent c : otherComponents)
         {
            stopComponent(c);
         }
         otherComponents.clear();
      }
   }

   /**
    * @param buffer
    * @return
    */
   private boolean checkThread(StringBuffer buffer)
   {
      boolean failedThread = false;

      Map<Thread, StackTraceElement[]> postThreads = Thread.getAllStackTraces();

      if (postThreads != null && previousThreads != null && postThreads.size() > previousThreads.size())
      {

         buffer.append("*********************************************************************************\n");
         buffer.append("LEAKING THREADS\n");

         for (Thread aliveThread : postThreads.keySet())
         {
            if (!isExpectedThread(aliveThread) && !previousThreads.containsKey(aliveThread))
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
    * if it's an expected thread... we will just move along ignoring it
    * @param thread
    * @return
    */
   private boolean isExpectedThread(Thread thread)
   {
      final String threadName = thread.getName();
      final ThreadGroup group = thread.getThreadGroup();
      final boolean isSystemThread = group != null && "system".equals(group.getName());
      final String javaVendor = System.getProperty("java.vendor");

      if (threadName.contains("SunPKCS11"))
      {
         return true;
      }
      else if (threadName.contains("Attach Listener"))
      {
         return true;
      }
      else if (isSystemThread && threadName.equals("process reaper"))
      {
         return true;
      }
      else if (javaVendor.contains("IBM") && threadName.equals("MemoryPoolMXBean notification dispatcher"))
      {
         return true;
      }
      else
      {
         for (StackTraceElement element : thread.getStackTrace())
         {
            if (element.getClassName().contains("org.jboss.byteman.agent.TransformListener"))
            {
               return true;
            }
         }
         return false;
      }
   }


   private void checkFilesUsage()
   {
      long timeout = System.currentTimeMillis() + 15000;

      while (AsynchronousFileImpl.getTotalMaxIO() != 0 && System.currentTimeMillis() > timeout)
      {
         try
         {
            Thread.sleep(100);
         }
         catch (Exception ignored)
         {
         }
      }

      int invmSize = InVMRegistry.instance.size();
      if (invmSize > 0)
      {
         InVMRegistry.instance.clear();
         log.info(threadDump("Thread dump"));
         fail("invm registry still had acceptors registered");
      }

      final int totalMaxIO = AsynchronousFileImpl.getTotalMaxIO();
      if (totalMaxIO != 0)
      {
         AsynchronousFileImpl.resetMaxAIO();
         Assert.fail("test did not close all its files " + totalMaxIO);
      }
   }

   private void cleanupPools()
   {
      OperationContextImpl.clearContext();

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

   protected static final byte[] autoEncode(final Object... args)
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
            throw HornetQMessageBundle.BUNDLE.autoConvertError(arg.getClass());
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
            throw HornetQMessageBundle.BUNDLE.autoConvertError(arg.getClass());
         }
      }

      return buffer.array();
   }

   protected static final void recreateDirectory(final String directory)
   {
      File file = new File(directory);
      deleteDirectory(file);
      file.mkdirs();
   }

   protected static final boolean deleteDirectory(final File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();
         int num = 5;
         int attempts = 0;
         while (files == null && (attempts < num))
         {
            try
            {
               Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
            }
            files = directory.list();
            attempts++;
         }

         for (String file : files)
         {
            File f = new File(directory, file);
            if (!deleteDirectory(f))
            {
               log.warn("Failed to clean up file: " + f.getAbsolutePath());
            }
         }
      }

      return directory.delete();
   }

   protected static final void copyRecursive(final File from, final File to) throws Exception
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
      ClientMessage message = session.createMessage(Message.TEXT_TYPE,
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
         Object o = DescribeJournal.newObjectEncoding(info);
         if (info.getUserRecordType() == JournalRecordIds.ADD_REF)
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

   protected final ServerLocator createInVMNonHALocator()
   {
      return createNonHALocator(false);
   }

   protected final ServerLocator createNettyNonHALocator()
   {
      return createNonHALocator(true);
   }

   protected final ServerLocator createNonHALocator(final boolean isNetty)
   {
      ServerLocator locatorWithoutHA =
               isNetty
                      ? HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY))
                      : HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      return addServerLocator(locatorWithoutHA);
   }

   protected static final void stopComponent(HornetQComponent component)
   {
      if (component == null)
         return;
      try
      {
         component.stop();
      }
      catch (Exception e)
      {
         // no-op
      }
   }

   protected static final void stopComponentOutputExceptions(HornetQComponent component)
   {
      if (component == null)
         return;
      try
      {
         component.stop();
      }
      catch (Exception e)
      {
         System.err.println("Exception closing " + component);
         e.printStackTrace();
      }
   }

   protected final ClientSessionFactory createSessionFactory(ServerLocator locator) throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();
      addSessionFactory(sf);
      return sf;
   }

   protected final HornetQServer addServer(final HornetQServer server)
   {
      if (server != null)
      {
         synchronized (servers)
         {
            servers.add(server);
         }
      }
      return server;
   }

   protected final ServerLocator addServerLocator(final ServerLocator locator)
   {
      if (locator != null)
      {
         synchronized (locators)
         {
            locators.add(locator);
         }
      }
      return locator;
   }

   protected final ClientSession addClientSession(final ClientSession session)
   {
      if (session != null)
      {
         synchronized (clientSessions)
         {
            clientSessions.add(session);
         }
      }
      return session;
   }

   protected final ClientConsumer addClientConsumer(final ClientConsumer consumer)
   {
      if (consumer != null)
      {
         synchronized (clientConsumers)
         {
            clientConsumers.add(consumer);
         }
      }
      return consumer;
   }

   protected final ClientProducer addClientProducer(final ClientProducer producer)
   {
      if (producer != null)
      {
         synchronized (clientProducers)
         {
            clientProducers.add(producer);
         }
      }
      return producer;
   }

   protected final void addHornetQComponent(final HornetQComponent component)
   {
      if (component != null)
      {
         synchronized (otherComponents)
         {
            otherComponents.add(component);
         }
      }
   }

   protected final ClientSessionFactory addSessionFactory(final ClientSessionFactory sf)
   {
      if (sf != null)
      {
         synchronized (sessionFactories)
         {
            sessionFactories.add(sf);
         }
      }
      return sf;
   }

   private void assertAllClientConsumersAreClosed()
   {
      synchronized (clientConsumers)
      {
         for (ClientConsumer cc : clientConsumers)
         {
            if (cc == null )
               continue;
            assertTrue(cc.isClosed());
         }
         clientConsumers.clear();
      }
   }

   private void assertAllClientSessionsAreClosed()
   {
      synchronized (clientSessions)
      {
         for (final ClientSession cs : clientSessions)
         {
            if (cs == null)
               continue;
            assertTrue(cs.isClosed());
         }
         clientSessions.clear();
      }
   }

   protected void closeAllSessionFactories()
   {
      synchronized (sessionFactories)
      {
         for (ClientSessionFactory sf : sessionFactories)
         {
            closeSessionFactory(sf);
            assert sf.isClosed();
         }
         sessionFactories.clear();
      }
   }

   protected void closeAllServerLocatorsFactories()
   {
      synchronized (locators)
      {
         for (ServerLocator locator : locators)
         {
            closeServerLocator(locator);
         }
         locators.clear();
      }
   }

   public static final void closeServerLocator(ServerLocator locator)
   {
      if (locator == null)
         return;
      try
      {
         locator.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static final void closeSessionFactory(final ClientSessionFactory sf)
   {
      if (sf == null)
         return;
      try
      {
         sf.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public static void crashAndWaitForFailure(HornetQServer server, ClientSession ...sessions) throws Exception
    {
       CountDownLatch latch = new CountDownLatch(sessions.length);
       for (ClientSession session : sessions)
       {
          CountDownSessionFailureListener listener = new CountDownSessionFailureListener(latch, session);
          session.addFailureListener(listener);
       }

       ClusterManager clusterManager = server.getClusterManager();
       clusterManager.flushExecutor();
       clusterManager.clear();
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


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected static interface HornetQAction
   {
      void run() throws Exception;
   }

   /**
    * Asserts that latch completes within a (rather large interval).
    * <p>
    * Use this instead of just calling {@code latch.await()}. Otherwise your test may hang the whole
    * test run if it fails to count-down the latch.
    * @param latch
    * @throws InterruptedException
    */
   public static void waitForLatch(CountDownLatch latch) throws InterruptedException
   {
      assertTrue("Latch has got to return within a minute", latch.await(1, TimeUnit.MINUTES));
   }
}
