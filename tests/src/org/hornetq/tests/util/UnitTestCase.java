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
import java.lang.ref.WeakReference;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.naming.Context;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import junit.framework.TestCase;

import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.utils.SimpleString;
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
   

   public static final String INVM_ACCEPTOR_FACTORY = "org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory";

   public static final String INVM_CONNECTOR_FACTORY = "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory";

   public static final String NETTY_ACCEPTOR_FACTORY = "org.hornetq.integration.transports.netty.NettyAcceptorFactory";

   public static final String NETTY_CONNECTOR_FACTORY = "org.hornetq.integration.transports.netty.NettyConnectorFactory";

   // Attributes ----------------------------------------------------

   private String testDir = System.getProperty("java.io.tmpdir", "/tmp") + "/hornetq-unit-test";

   
   
   
   // Static --------------------------------------------------------

   public static void forceGC()
   {
      WeakReference<Object> dumbReference = new WeakReference<Object>(new Object());
      // A loop that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
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
   public static void checkWeakReferences(WeakReference<?>... references)
   {

      int i = 0;
      boolean hasValue = false;

      do
      {
         hasValue = false;

         if (i > 0)
         {
            forceGC();
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
         assertNull(ref.get());
      }
   }


   public static String threadDump(String msg)
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);
      
      
      Map<Thread, StackTraceElement[]> stackTrace = Thread.getAllStackTraces();

      out.println("*******************************************************************************");
      out.println("Complete Thread dump" + msg);

      for (Map.Entry<Thread, StackTraceElement[]> el : stackTrace.entrySet())
      {
         out.println("===============================================================================");
         out.println("Thread " + el.getKey() + " name = " + el.getKey().getName() + " group = " + el.getKey().getThreadGroup());
         out.println();
         for (StackTraceElement traceEl : el.getValue())
         {
            out.println(traceEl);
         }
      }
      
      out.println("===============================================================================");
      out.println("End Thread dump" + msg);
      out.println("*******************************************************************************");
      
      
      return str.toString();
   }
   
   public static String dumpBytes(byte[] bytes)
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

   public static String dumbBytesHex(final byte[] buffer, int bytesPerLine)
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

   public static void assertEqualsByteArrays(byte[] expected, byte[] actual)
   {
      assertEquals(expected.length, actual.length);
      for (int i = 0; i < expected.length; i++)
      {
         assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertEqualsByteArrays(int length, byte[] expected, byte[] actual)
   {
      // we check only for the given length (the arrays might be
      // larger)
      assertTrue(expected.length >= length);
      assertTrue(actual.length >= length);
      for (int i = 0; i < length; i++)
      {
         assertEquals("byte at index " + i, expected[i], actual[i]);
      }
   }

   public static void assertSameXids(List<Xid> expected, List<Xid> actual)
   {
      assertNotNull(expected);
      assertNotNull(actual);
      assertEquals(expected.size(), actual.size());

      for (int i = 0; i < expected.size(); i++)
      {
         Xid expectedXid = expected.get(i);
         Xid actualXid = actual.get(i);
         UnitTestCase.assertEqualsByteArrays(expectedXid.getBranchQualifier(), actualXid.getBranchQualifier());
         assertEquals(expectedXid.getFormatId(), actualXid.getFormatId());
         UnitTestCase.assertEqualsByteArrays(expectedXid.getGlobalTransactionId(), actualXid.getGlobalTransactionId());
      }
   }

   protected static void checkNoBinding(Context context, String binding)
   {
      try
      {
         context.lookup(binding);
         fail("there must be no resource to look up for " + binding);
      }
      catch (Exception e)
      {
      }
   }

   protected static Object checkBinding(Context context, String binding) throws Exception
   {
      Object o = context.lookup(binding);
      assertNotNull(o);
      return o;
   }
   
   protected static void checkFreePort(int... ports)
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
            fail("port " + port + " is already bound");
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

   protected void clearData(String testDir)
   {
      // Need to delete the root

      File file = new File(testDir);
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
   protected String getJournalDir()
   {
      return getJournalDir(testDir);
   }

   protected String getJournalDir(String testDir)
   {
      return testDir + "/journal";
   }
   
   protected String getJournalDir(int index, boolean backup)
   {
      String dir =  getJournalDir(testDir) + index + "-" + (backup ? "B" : "L");

      return dir;
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir()
   {
      return getBindingsDir(testDir);
   }
   
   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(String testDir)
   {
      return testDir + "/bindings";
   }

   /**
    * @return the bindingsDir
    */
   protected String getBindingsDir(int index, boolean backup)
   {
      return getBindingsDir(testDir) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the pageDir
    */
   protected String getPageDir()
   {
      return getPageDir(testDir);
   }

   /**
    * @return the pageDir
    */
   protected String getPageDir(String testDir)
   {
      return testDir + "/page";
   }
   
   protected String getPageDir(int index, boolean backup)
   {
      return getPageDir(testDir) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the largeMessagesDir
    */
   protected String getLargeMessagesDir()
   {
      return getLargeMessagesDir(testDir);
   }

   /**
    * @return the largeMessagesDir
    */
   protected String getLargeMessagesDir(String testDir)
   {
      return testDir + "/large-msg";
   }
   
   protected String getLargeMessagesDir(int index, boolean backup)
   {
      return getLargeMessagesDir(testDir) + index + "-" + (backup ? "B" : "L");
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir()
   {
      return getClientLargeMessagesDir(testDir);
   }

   /**
    * @return the clientLargeMessagesDir
    */
   protected String getClientLargeMessagesDir(String testDir)
   {
      return testDir + "/client-large-msg";
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir()
   {
      return getTemporaryDir(testDir);
   }

   /**
    * @return the temporaryDir
    */
   protected String getTemporaryDir(String testDir)
   {
      return testDir + "/temp";
   }

   protected static void expectHornetQException(String message, int errorCode, HornetQAction action)
   {
      try
      {
         action.run();
         fail(message);
      }
      catch (Exception e)
      {
         assertTrue(e instanceof HornetQException);
         assertEquals(errorCode, ((HornetQException)e).getCode());
      }
   }
   
   protected static void expectHornetQException(int errorCode, HornetQAction action)
   {
      expectHornetQException("must throw a HornetQException with the expected errorCode: " + errorCode, errorCode, action);
   }
   
   protected static void expectXAException(int errorCode, HornetQAction action)
   {
      try
      {
         action.run();
         fail("must throw a XAException with the expected errorCode: " + errorCode);
      }
      catch (Exception e)
      {
         assertTrue(e instanceof XAException);
         assertEquals(errorCode, ((XAException)e).errorCode);
      }
   }
   
   
   public static byte getSamplebyte(final long position)
   {
      return (byte)('a' + (position) % ('z' - 'a' + 1));
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
               return getSamplebyte(count - 1);
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
   protected void validateGettersAndSetters(Object pojo, String...IgnoredProperties) throws Exception
   {
      HashSet<String> ignoreSet = new HashSet<String>();
      
      for (String ignore: IgnoredProperties)
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
         else
         if (value != null & prop.getWriteMethod() != null && prop.getReadMethod() != null && !ignoreSet.contains(prop.getName()))
         {
            System.out.println("Validating " + prop.getName() + " type = " + prop.getPropertyType());
            prop.getWriteMethod().invoke(pojo, value);
         
            assertEquals("Property " + prop.getName(), value, prop.getReadMethod().invoke(pojo));
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      deleteDirectory(new File(getTestDir()));
      
      InVMRegistry.instance.clear();
      
      // checkFreePort(TransportConstants.DEFAULT_PORT);
      
      log.info("###### starting test " + this.getClass().getName() + "." + this.getName());
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      deleteDirectory(new File(getTestDir()));

      assertEquals(0, InVMRegistry.instance.size());
      
      if (AsynchronousFileImpl.getTotalMaxIO() != 0)
      {
         AsynchronousFileImpl.resetMaxAIO();
         fail("test did not close all its files " + AsynchronousFileImpl.getTotalMaxIO());
      }

      super.tearDown();
   }

   protected byte[] autoEncode(Object... args)
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

   protected void recreateDirectory(String directory)
   {
      File file = new File(directory);
      deleteDirectory(file);
      file.mkdirs();
   }

   protected boolean deleteDirectory(File directory)
   {
      if (directory.isDirectory())
      {
         String[] files = directory.list();

         for (int j = 0; j < files.length; j++)
         {
            if (!deleteDirectory(new File(directory, files[j])))
            {
               return false;
            }
         }
      }

      return directory.delete();
   }

   protected void copyRecursive(File from, File to) throws Exception
   {
      if (from.isDirectory())
      {
         if (!to.exists())
         {
            to.mkdir();
         }

         String[] subs = from.list();

         for (int i = 0; i < subs.length; i++)
         {
            copyRecursive(new File(from, subs[i]), new File(to, subs[i]));
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

   protected void assertRefListsIdenticalRefs(List<MessageReference> l1, List<MessageReference> l2)
   {
      if (l1.size() != l2.size())
      {
         fail("Lists different sizes: " + l1.size() + ", " + l2.size());
      }

      Iterator<MessageReference> iter1 = l1.iterator();
      Iterator<MessageReference> iter2 = l2.iterator();

      while (iter1.hasNext())
      {
         MessageReference o1 = iter1.next();
         MessageReference o2 = iter2.next();

         assertTrue(o1 == o2);
      }
   }

   protected ServerMessage generateMessage(long id)
   {
      ServerMessage message = new ServerMessageImpl((byte)0,
                                                    true,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    (byte)4,
                                                    ChannelBuffers.dynamicBuffer(1024));

      message.setMessageID(id);

      message.getBody().writeString(UUID.randomUUID().toString());
      
      message.setDestination(new SimpleString("foo"));

      return message;
   }

   protected MessageReference generateReference(Queue queue, long id)
   {
      ServerMessage message = generateMessage(id);

      return message.createReference(queue);
   }

   protected int calculateRecordSize(int size, int alignment)
   {
      return ((size / alignment) + (size % alignment != 0 ? 1 : 0)) * alignment;
   }

   protected ClientMessage createTextMessage(String s, ClientSession clientSession)
   {
      return createTextMessage(s, true, clientSession);
   }

   protected ClientMessage createTextMessage(String s, boolean durable, ClientSession clientSession)
   {
      ClientMessage message = clientSession.createClientMessage(HornetQTextMessage.TYPE,
                                                                durable,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                (byte)1);
      message.getBody().writeString(s);
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
      
      for (QueueBinding qBinding: bindings)
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
         if ((binding instanceof LocalQueueBinding))
         {
            bindingsFound.add((QueueBinding)binding);
         }
      }
      return bindingsFound;
   }


   
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected static interface HornetQAction
   {
      void run() throws Exception;
   }

}
