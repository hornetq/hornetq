/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.persistence.impl.journal;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.utils.Base64;
import org.hornetq.utils.ExecutorFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.hornetq.core.persistence.impl.journal.JournalStorageManager.*;

/**
 * Read the journal, page, and large-message data from a stopped instance of HornetQ and save it in an XML format to
 * a file.  It uses the StAX <code>javax.xml.stream.XMLStreamWriter</code> for speed and simplicity.  Output can be
 * read by <code>org.hornetq.core.persistence.impl.journal.XmlDataImporter</code>.
 *
 * @author Justin Bertram
 */
public class XmlDataExporter
{
   // Constants -----------------------------------------------------

   public static final Long LARGE_MESSAGE_CHUNK_SIZE = 1000L;

   // Attributes ----------------------------------------------------

   private static final Logger log = Logger.getLogger(XmlDataExporter.class);

   private JournalStorageManager storageManager;

   private Configuration config;

   private XMLStreamWriter xmlWriter;

   // an inner map of message refs hashed by the queue ID to which they belong and then hashed by their record ID
   private Map<Long, HashMap<Long, ReferenceDescribe>> messageRefs;

   // map of all message records hashed by their record ID (which will match the record ID of the message refs)
   private HashMap<Long, Message> messages;

   private Map<Long, Set<PagePosition>> cursorRecords;

   private Set<Long> pgTXs;

   HashMap<Long, PersistentQueueBindingEncoding> queueBindings;

   long messagesPrinted = 0L;

   long bindingsPrinted = 0L;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public XmlDataExporter(OutputStream out, String bindingsDir, String journalDir, String pagingDir, String largeMessagesDir)
   {
      config = new ConfigurationImpl();
      config.setBindingsDirectory(bindingsDir);
      config.setJournalDirectory(journalDir);
      config.setPagingDirectory(pagingDir);
      config.setLargeMessagesDirectory(largeMessagesDir);
      config.setJournalType(JournalType.NIO);
      final ExecutorService executor = Executors.newFixedThreadPool(1);
      ExecutorFactory executorFactory = new ExecutorFactory()
      {
         public Executor getExecutor()
         {
            return executor;
         }
      };

      storageManager = new JournalStorageManager(config, executorFactory);

      messageRefs = new HashMap<Long, HashMap<Long, ReferenceDescribe>>();

      messages = new HashMap<Long, Message>();

      cursorRecords = new HashMap<Long, Set<PagePosition>>();

      pgTXs = new HashSet<Long>();

      queueBindings = new HashMap<Long, PersistentQueueBindingEncoding>();

      try
      {
         XMLOutputFactory factory = XMLOutputFactory.newInstance();
         XMLStreamWriter rawXmlWriter = factory.createXMLStreamWriter(out);
         PrettyPrintHandler handler = new PrettyPrintHandler(rawXmlWriter);
         xmlWriter = (XMLStreamWriter) Proxy.newProxyInstance(
               XMLStreamWriter.class.getClassLoader(),
               new Class[]{XMLStreamWriter.class},
               handler);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      if (arg.length < 4)
      {
         System.out.println("Use: java -cp hornetq-core.jar <bindings directory> <message directory> <page directory> <large-message directory>");
         System.exit(-1);
      }

      try
      {
         XmlDataExporter xmlDataExporter = new XmlDataExporter(System.out, arg[0], arg[1], arg[2], arg[3]);
         xmlDataExporter.writeXMLData();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public void writeXMLData() throws Exception
   {
      long start = System.currentTimeMillis();
      getBindings();
      processMessageJournal();
      printDataAsXML();
      log.debug("\n\nProcessing took: " + (System.currentTimeMillis() - start) + "ms");
      log.debug("Output " + messagesPrinted + " messages and " + bindingsPrinted + " bindings.");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Read through the message journal and stuff all the events/data we care about into local data structures.  We'll
    * use this data later to print all the right information.
    *
    * @throws Exception will be thrown if anything goes wrong reading the journal
    */
   private void processMessageJournal() throws Exception
   {
      ArrayList<RecordInfo> acks = new ArrayList<RecordInfo>();

      List<RecordInfo> records = new LinkedList<RecordInfo>();

      Journal messageJournal = storageManager.getMessageJournal();

      log.debug("Reading journal from " + config.getJournalDirectory());

      messageJournal.start();

      ((JournalImpl) messageJournal).load(records, null, null, false);

      for (RecordInfo info : records)
      {
         byte[] data = info.data;

         HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);

         Object o = JournalStorageManager.newObjectEncoding(info, storageManager);
         if (info.getUserRecordType() == JournalStorageManager.ADD_MESSAGE)
         {
            messages.put(info.id, ((MessageDescribe) o).msg);
         }
         else if (info.getUserRecordType() == JournalStorageManager.ADD_LARGE_MESSAGE)
         {
            messages.put(info.id, ((MessageDescribe) o).msg);
         }
         else if (info.getUserRecordType() == JournalStorageManager.ADD_REF)
         {
            ReferenceDescribe ref = (ReferenceDescribe) o;
            HashMap<Long, ReferenceDescribe> map = messageRefs.get(info.id);
            if (map == null)
            {
               HashMap<Long, ReferenceDescribe> newMap = new HashMap<Long, ReferenceDescribe>();
               newMap.put(ref.refEncoding.queueID, ref);
               messageRefs.put(info.id, newMap);
            }
            else
            {
               map.put(ref.refEncoding.queueID, ref);
            }
         }
         else if (info.getUserRecordType() == JournalStorageManager.ACKNOWLEDGE_REF)
         {
            acks.add(info);
         }
         else if (info.userRecordType == JournalStorageManager.ACKNOWLEDGE_CURSOR)
         {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
            encoding.decode(buff);

            Set<PagePosition> set = cursorRecords.get(encoding.queueID);

            if (set == null)
            {
               set = new HashSet<PagePosition>();
               cursorRecords.put(encoding.queueID, set);
            }

            set.add(encoding.position);
         }
         else if (info.userRecordType == JournalStorageManager.PAGE_TRANSACTION)
         {
            if (info.isUpdate)
            {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buff);
               pgTXs.add(pageUpdate.pageTX);
            }
            else
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(info.id);
               pgTXs.add(pageTransactionInfo.getTransactionID());
            }
         }
      }

      messageJournal.stop();

      removeAcked(acks);
   }

   /**
    * Go back through the messages and message refs we found in the journal and remove the ones that have been acked.
    *
    * @param acks the list of ack records we got from the journal
    */
   private void removeAcked(ArrayList<RecordInfo> acks)
   {
      for (RecordInfo info : acks)
      {
         AckDescribe ack = (AckDescribe) JournalStorageManager.newObjectEncoding(info, null);
         HashMap<Long, ReferenceDescribe> referenceDescribeHashMap = messageRefs.get(info.id);
         referenceDescribeHashMap.remove(ack.refEncoding.queueID);
         if (referenceDescribeHashMap.size() == 0)
         {
            messages.remove(info.id);
            messageRefs.remove(info.id);
         }
      }
   }

   /**
    * Open the bindings journal and extract all bindings data.
    *
    * @throws Exception will be thrown if anything goes wrong reading the bindings journal
    */
   private void getBindings() throws Exception
   {
      List<RecordInfo> records = new LinkedList<RecordInfo>();

      Journal bindingsJournal = storageManager.getBindingsJournal();

      bindingsJournal.start();

      log.debug("Reading bindings journal from " + config.getBindingsDirectory());

      ((JournalImpl) bindingsJournal).load(records, null, null, false);

      for (RecordInfo info : records)
      {
         if (info.getUserRecordType() == JournalStorageManager.QUEUE_BINDING_RECORD)
         {
            PersistentQueueBindingEncoding bindingEncoding = (PersistentQueueBindingEncoding) JournalStorageManager.newObjectEncoding(info, null);
            queueBindings.put(bindingEncoding.getId(), bindingEncoding);
         }
      }

      bindingsJournal.stop();
   }

   private void printDataAsXML()
   {
      try
      {
         xmlWriter.writeStartDocument(XmlDataConstants.XML_VERSION);
         xmlWriter.writeStartElement(XmlDataConstants.DOCUMENT_PARENT);
         printBindingsAsXML();
         printAllMessagesAsXML();
         xmlWriter.writeEndElement(); // end DOCUMENT_PARENT
         xmlWriter.writeEndDocument();
         xmlWriter.flush();
         xmlWriter.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private void printBindingsAsXML() throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.BINDINGS_PARENT);
      for (Map.Entry<Long, PersistentQueueBindingEncoding> queueBindingEncodingEntry : queueBindings.entrySet())
      {
         PersistentQueueBindingEncoding bindingEncoding = queueBindings.get(queueBindingEncodingEntry.getKey());
         xmlWriter.writeEmptyElement(XmlDataConstants.BINDINGS_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_ADDRESS, bindingEncoding.getAddress().toString());
         String filter = "";
         if (bindingEncoding.getFilterString() != null)
         {
            filter = bindingEncoding.getFilterString().toString();
         }
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_FILTER_STRING, filter);
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_QUEUE_NAME, bindingEncoding.getQueueName().toString());
         xmlWriter.writeAttribute(XmlDataConstants.BINDING_ID, Long.toString(bindingEncoding.getId()));
         bindingsPrinted++;
      }
      xmlWriter.writeEndElement(); // end BINDINGS_PARENT
   }

   private void printAllMessagesAsXML() throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGES_PARENT);

      // Order here is important.  We must process the messages from the journal before we process those from the page
      // files in order to get the messages in the right order.
      for (Map.Entry<Long, Message> messageMapEntry : messages.entrySet())
      {
         printSingleMessageAsXML((ServerMessage) messageMapEntry.getValue(), extractQueueNames(messageRefs.get(messageMapEntry.getKey())));
      }

      printPagedMessagesAsXML();

      xmlWriter.writeEndElement(); // end "messages"
   }

   /**
    * Reads from the page files and prints messages as it finds them (making sure to check acks and transactions
    * from the journal).
    */
   private void printPagedMessagesAsXML()
   {
      try
      {
         ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
         final ExecutorService executor = Executors.newFixedThreadPool(10);
         ExecutorFactory executorFactory = new ExecutorFactory()
         {
            public Executor getExecutor()
            {
               return executor;
            }
         };
         PagingStoreFactory pageStoreFactory = new PagingStoreFactoryNIO(config.getPagingDirectory(), 1000l, scheduled, executorFactory, false, null);
         HierarchicalRepository<AddressSettings> addressSettingsRepository = new HierarchicalObjectRepository<AddressSettings>();
         addressSettingsRepository.setDefault(new AddressSettings());
         StorageManager sm = new NullStorageManager();
         PagingManager manager = new PagingManagerImpl(pageStoreFactory, sm, addressSettingsRepository);

         manager.start();

         SimpleString stores[] = manager.getStoreNames();

         for (SimpleString store : stores)
         {
            PagingStore pageStore = manager.getPageStore(store);
            String folder = null;

            if (pageStore != null)
            {
               folder = pageStore.getFolder();
            }
            log.debug("Reading page store " + store + " folder = " + folder);

            int pageId = (int) pageStore.getFirstPage();
            for (int i = 0; i < pageStore.getNumberOfPages(); i++)
            {
               log.debug("Reading page " + pageId);
               Page page = pageStore.createPage(pageId);
               page.open();
               List<PagedMessage> messages = page.read(sm);
               page.close();

               int messageId = 0;

               for (PagedMessage message : messages)
               {
                  message.initMessage(sm);
                  long queueIDs[] = message.getQueueIDs();
                  List<String> queueNames = new ArrayList<String>();
                  for (long queueID : queueIDs)
                  {
                     PagePosition posCheck = new PagePositionImpl(pageId, messageId);

                     boolean acked = false;

                     Set<PagePosition> positions = cursorRecords.get(queueID);
                     if (positions != null)
                     {
                        acked = positions.contains(posCheck);
                     }

                     if (!acked)
                     {
                        queueNames.add(queueBindings.get(queueID).getQueueName().toString());
                     }
                  }

                  if (queueNames.size() > 0 && (message.getTransactionID() == -1 || pgTXs.contains(message.getTransactionID())))
                  {
                     printSingleMessageAsXML(message.getMessage(), queueNames);
                  }

                  messageId++;
               }

               pageId++;
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private void printSingleMessageAsXML(ServerMessage message, List<String> queues) throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGES_CHILD);
      printMessageAttributes(message);
      printMessageProperties(message);
      printMessageQueues(queues);
      printMessageBody(message);
      xmlWriter.writeEndElement(); // end MESSAGES_CHILD
      messagesPrinted++;
   }

   private void printMessageBody(ServerMessage message) throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.MESSAGE_BODY);

      if (message.isLargeMessage())
      {
         printLargeMessageBody((LargeServerMessage) message);
      }
      else
      {
         xmlWriter.writeCData(encode(message.getBodyBuffer().toByteBuffer().array()));
      }
      xmlWriter.writeEndElement(); // end MESSAGE_BODY
   }

   private void printLargeMessageBody(LargeServerMessage message) throws XMLStreamException
   {
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_IS_LARGE, Boolean.TRUE.toString());
      BodyEncoder encoder = null;

      try
      {
         encoder = message.getBodyEncoder();
         encoder.open();
         long totalBytesWritten = 0;
         Long bufferSize;
         long bodySize = encoder.getLargeBodySize();
         for (long i = 0; i < bodySize; i += LARGE_MESSAGE_CHUNK_SIZE)
         {
            Long remainder = bodySize - totalBytesWritten;
            if (remainder >= LARGE_MESSAGE_CHUNK_SIZE)
            {
               bufferSize = LARGE_MESSAGE_CHUNK_SIZE;
            }
            else
            {
               bufferSize = remainder;
            }
            HornetQBuffer buffer = HornetQBuffers.fixedBuffer(bufferSize.intValue());
            encoder.encode(buffer, bufferSize.intValue());
            xmlWriter.writeCData(encode(buffer.toByteBuffer().array()));
            totalBytesWritten += bufferSize;
         }
         encoder.close();
      }
      catch (HornetQException e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (encoder != null)
         {
            try
            {
               encoder.close();
            }
            catch (HornetQException e)
            {
               e.printStackTrace();
            }
         }
      }
   }

   private void printMessageQueues(List<String> queues) throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.QUEUES_PARENT);
      for (String queueName : queues)
      {
         xmlWriter.writeEmptyElement(XmlDataConstants.QUEUES_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.QUEUE_NAME, queueName);
      }
      xmlWriter.writeEndElement(); // end QUEUES_PARENT
   }

   private void printMessageProperties(ServerMessage message) throws XMLStreamException
   {
      xmlWriter.writeStartElement(XmlDataConstants.PROPERTIES_PARENT);
      for (SimpleString key : message.getPropertyNames())
      {
         Object value = message.getObjectProperty(key);
         xmlWriter.writeEmptyElement(XmlDataConstants.PROPERTIES_CHILD);
         xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_NAME, key.toString());
         if (value instanceof byte[])
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_VALUE, encode((byte[]) value));
         }
         else
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_VALUE, value.toString());
         }

         if (value instanceof Boolean)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BOOLEAN);
         }
         else if (value instanceof Byte)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BYTE);
         }
         else if (value instanceof Short)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_SHORT);
         }
         else if (value instanceof Integer)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_INTEGER);
         }
         else if (value instanceof Long)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_LONG);
         }
         else if (value instanceof Float)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_FLOAT);
         }
         else if (value instanceof Double)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_DOUBLE);
         }
         else if (value instanceof String)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_STRING);
         }
         else if (value instanceof SimpleString)
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING);
         }
         else if (value instanceof byte[])
         {
            xmlWriter.writeAttribute(XmlDataConstants.PROPERTY_TYPE, XmlDataConstants.PROPERTY_TYPE_BYTES);
         }
      }
      xmlWriter.writeEndElement(); // end PROPERTIES_PARENT
   }

   private void printMessageAttributes(ServerMessage message) throws XMLStreamException
   {
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_ID, Long.toString(message.getMessageID()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_PRIORITY, Byte.toString(message.getPriority()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_EXPIRATION, Long.toString(message.getExpiration()));
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_TIMESTAMP, Long.toString(message.getTimestamp()));
      byte rawType = message.getType();
      String prettyType = XmlDataConstants.DEFAULT_TYPE_PRETTY;
      if (rawType == Message.BYTES_TYPE)
      {
         prettyType = XmlDataConstants.BYTES_TYPE_PRETTY;
      }
      else if (rawType == Message.MAP_TYPE)
      {
         prettyType = XmlDataConstants.MAP_TYPE_PRETTY;
      }
      else if (rawType == Message.OBJECT_TYPE)
      {
         prettyType = XmlDataConstants.OBJECT_TYPE_PRETTY;
      }
      else if (rawType == Message.STREAM_TYPE)
      {
         prettyType = XmlDataConstants.STREAM_TYPE_PRETTY;
      }
      else if (rawType == Message.TEXT_TYPE)
      {
         prettyType = XmlDataConstants.TEXT_TYPE_PRETTY;
      }
      xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_TYPE, prettyType);
      if (message.getUserID() != null)
      {
         xmlWriter.writeAttribute(XmlDataConstants.MESSAGE_USER_ID, message.getUserID().toString());
      }
   }

   private List<String> extractQueueNames(HashMap<Long, ReferenceDescribe> refMap)
   {
      List<String> queues = new ArrayList<String>();
      for (ReferenceDescribe ref : refMap.values())
      {
         queues.add(queueBindings.get(ref.refEncoding.queueID).getQueueName().toString());
      }
      return queues;
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Inner classes -------------------------------------------------

   /**
    * Proxy to handle indenting the XML since <code>javax.xml.stream.XMLStreamWriter</code> doesn't support that.
    */
   class PrettyPrintHandler implements InvocationHandler
   {
      private XMLStreamWriter target;

      private int depth = 0;

      private final char INDENT_CHAR = ' ';

      private final String LINE_SEPARATOR = System.getProperty("line.separator");


      public PrettyPrintHandler(XMLStreamWriter target)
      {
         this.target = target;
      }

      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
      {
         String m = method.getName();

         if ("writeStartElement".equals(m))
         {
            target.writeCharacters(LINE_SEPARATOR);
            target.writeCharacters(indent(depth));

            depth++;
         }
         else if ("writeEndElement".equals(m))
         {
            depth--;

            target.writeCharacters(LINE_SEPARATOR);
            target.writeCharacters(indent(depth));
         }
         else if ("writeEmptyElement".equals(m) || "writeCData".equals(m))
         {
            target.writeCharacters(LINE_SEPARATOR);
            target.writeCharacters(indent(depth));
         }

         method.invoke(target, args);

         return null;
      }

      private String indent(int depth)
      {
         depth *= 3; // level of indentation
         char[] output = new char[depth];
         while (depth-- > 0)
         {
            output[depth] = INDENT_CHAR;
         }
         return new String(output);
      }
   }
}