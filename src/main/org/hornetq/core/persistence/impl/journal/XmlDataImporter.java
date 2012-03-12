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


import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientRequestor;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.utils.Base64;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Read XML output from <code>org.hornetq.core.persistence.impl.journal.XmlDataExporter</code>, create a core session, and
 * send the messages to a running instance of HornetQ.  It uses the StAX <code>javax.xml.stream.XMLStreamReader</code>
 * for speed and simplicity.
 *
 * @author Justin Bertram
 */
public class XmlDataImporter
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final Logger log = Logger.getLogger(XmlDataImporter.class);

   XMLStreamReader reader;

   ClientSession session;

   // this session is really only needed if the "session" variable does not auto-commit sends
   ClientSession managementSession;

   boolean localSession = false;

   Map<String, String> addressMap = new HashMap<String, String>();

   String tempFileName = "";

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * This is the normal constructor for programmatic access to the <code>org.hornetq.core.persistence.impl.journal.XmlDataImporter</code>
    * if the session passed in uses auto-commit for sends.  If the session needs to be transactional then use the
    * constructor which takes 2 sessions.
    *
    * @param inputStream the stream from which to read the XML for import
    * @param session used for sending messages, must use auto-commit for sends
    */
   public XmlDataImporter(InputStream inputStream, ClientSession session)
   {
      this(inputStream, session, null);
   }

   /**
    * This is the constructor to use if you wish to import all messages transactionally.  Pass in a session which doesn't
    * use auto-commit for sends, and one that does (for management operations necessary during import).
    *
    * @param inputStream the stream from which to read the XML for import
    * @param session used for sending messages, doesn't need to auto-commit sends
    * @param managementSession used for management queries, must use auto-commit for sends
    */
   public XmlDataImporter(InputStream inputStream, ClientSession session, ClientSession managementSession)
   {
      try
      {
         reader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
         this.session = session;
         if (managementSession != null)
         {
            this.managementSession = managementSession;
         }
         else
         {
            this.managementSession = session;
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public XmlDataImporter(InputStream inputStream, String host, String port, boolean transactional)
   {
      try
      {
         reader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
         HashMap<String, Object> connectionParams = new HashMap<String, Object>();
         connectionParams.put(TransportConstants.HOST_PROP_NAME, host);
         connectionParams.put(TransportConstants.PORT_PROP_NAME, port);
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), connectionParams));
         ClientSessionFactory sf = serverLocator.createSessionFactory();
         session = sf.createSession(false, !transactional, true);
         if (transactional) {
            managementSession = sf.createSession(false, true, true);
         }
         localSession = true;
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public XmlDataImporter(String inputFile, String host, String port, boolean transactional) throws FileNotFoundException
   {
      this(new FileInputStream(inputFile), host, port, transactional);
   }

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      if (arg.length < 3)
      {
         System.out.println("Use: java -cp hornetq-core.jar " + XmlDataImporter.class + " <inputFile> <host> <port> [<transactional>]");
         System.exit(-1);
      }

      try
      {
         XmlDataImporter xmlDataImporter = new XmlDataImporter(arg[0], arg[1], arg[2], (arg.length > 3 && Boolean.parseBoolean(arg[3])));
         xmlDataImporter.processXml();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   public void processXml() throws Exception
   {
      try
      {
         while (reader.hasNext())
         {
            log.debug("EVENT:[" + reader.getLocation().getLineNumber() + "][" + reader.getLocation().getColumnNumber() + "] ");
            if (reader.getEventType() == XMLStreamConstants.START_ELEMENT)
            {
               if (XmlDataConstants.BINDINGS_CHILD.equals(reader.getLocalName()))
               {
                  bindQueue();
               }
               if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName()))
               {
                  processMessage();
               }
            }
            reader.next();
         }

         if (!session.isAutoCommitSends())
         {
            session.commit();
         }
      }
      finally
      {
         // if the session was created in our constructor then close it (otherwise the caller will close it)
         if (localSession)
         {
            session.close();
         }
      }
   }

   private void processMessage() throws Exception
   {
      Byte type = 0;
      Byte priority = 0;
      Long expiration = 0L;
      Long timestamp = 0L;
      org.hornetq.utils.UUID userId = null;
      ArrayList<String> queues = new ArrayList<String>();

      // get message's attributes
      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.MESSAGE_TYPE.equals(attributeName))
         {
            type = getMessageType(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_PRIORITY.equals(attributeName))
         {
            priority = Byte.parseByte(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_EXPIRATION.equals(attributeName))
         {
            expiration = Long.parseLong(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_TIMESTAMP.equals(attributeName))
         {
            timestamp = Long.parseLong(reader.getAttributeValue(i));
         }
         else if (XmlDataConstants.MESSAGE_USER_ID.equals(attributeName))
         {
            userId = org.hornetq.utils.UUIDGenerator.getInstance().generateUUID();
         }
      }

      Message message = session.createMessage(type, true, expiration, timestamp, priority);
      message.setUserID(userId);

      boolean endLoop = false;

      // loop through the XML and gather up all the message's data (i.e. body, properties, queues, etc.)
      while (reader.hasNext())
      {
         switch (reader.getEventType())
         {
            case XMLStreamConstants.START_ELEMENT:
               if (XmlDataConstants.MESSAGE_BODY.equals(reader.getLocalName()))
               {
                  processMessageBody(message);
               }
               else if (XmlDataConstants.PROPERTIES_CHILD.equals(reader.getLocalName()))
               {
                  processMessageProperties(message);
               }
               else if (XmlDataConstants.QUEUES_CHILD.equals(reader.getLocalName()))
               {
                  processMessageQueues(queues);
               }
               break;
            case XMLStreamConstants.END_ELEMENT:
               if (XmlDataConstants.MESSAGES_CHILD.equals(reader.getLocalName()))
               {
                  endLoop = true;
               }
               break;
         }
         if (endLoop)
         {
            break;
         }
         reader.next();
      }

      sendMessage(queues, message);
   }

   private Byte getMessageType(String value)
   {
      Byte type = Message.DEFAULT_TYPE;
      if (value.equals(XmlDataConstants.DEFAULT_TYPE_PRETTY))
      {
         type = Message.DEFAULT_TYPE;
      }
      else if (value.equals(XmlDataConstants.BYTES_TYPE_PRETTY))
      {
         type = Message.BYTES_TYPE;
      }
      else if (value.equals(XmlDataConstants.MAP_TYPE_PRETTY))
      {
         type = Message.MAP_TYPE;
      }
      else if (value.equals(XmlDataConstants.OBJECT_TYPE_PRETTY))
      {
         type = Message.OBJECT_TYPE;
      }
      else if (value.equals(XmlDataConstants.STREAM_TYPE_PRETTY))
      {
         type = Message.STREAM_TYPE;
      }
      else if (value.equals(XmlDataConstants.TEXT_TYPE_PRETTY))
      {
         type = Message.TEXT_TYPE;
      }
      return type;
   }

   private void sendMessage(ArrayList<String> queues, Message message) throws Exception
   {
      StringBuilder logMessage = new StringBuilder();
      String destination = addressMap.get(queues.get(0));

      logMessage.append("Sending ").append(message).append(" to address: ").append(destination).append("; routed to queues: ");
      ByteBuffer buffer = ByteBuffer.allocate(queues.size() * 8);

      for (String queue : queues)
      {
         // Get the ID of the queues involved so the message can be routed properly.  This is done because we cannot
         // send directly to a queue, we have to send to an address instead but not all the queues related to the
         // address may need the message
         ClientRequestor requestor = new ClientRequestor(managementSession, "jms.queue.hornetq.management");
         ClientMessage managementMessage = managementSession.createMessage(false);
         ManagementHelper.putAttribute(managementMessage, "core.queue." + queue, "ID");
         managementSession.start();
         ClientMessage reply = requestor.request(managementMessage);
         long queueID = (Integer) ManagementHelper.getResult(reply);
         logMessage.append(queue).append(", ");
         buffer.putLong(queueID);
      }

      logMessage.delete(logMessage.length() - 2, logMessage.length()); // take off the trailing comma
      log.debug(logMessage);

      message.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, buffer.array());
      ClientProducer producer = session.createProducer(destination);
      producer.send(message);
      producer.close();

      if (tempFileName.length() > 0)
      {
         File tempFile = new File(tempFileName);
         if (!tempFile.delete())
         {
            log.warn("Could not delete: " + tempFileName);
         }
      }
   }

   private void processMessageQueues(ArrayList<String> queues)
   {
      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         if (XmlDataConstants.QUEUE_NAME.equals(reader.getAttributeLocalName(i)))
         {
            queues.add(reader.getAttributeValue(i));
         }
      }
   }

   private void processMessageProperties(Message message)
   {
      String key = "";
      String value = "";
      String propertyType = "";

      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.PROPERTY_NAME.equals(attributeName))
         {
            key = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.PROPERTY_VALUE.equals(attributeName))
         {
            value = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.PROPERTY_TYPE.equals(attributeName))
         {
            propertyType = reader.getAttributeValue(i);
         }
      }

      if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_SHORT))
      {
         message.putShortProperty(key, Short.parseShort(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_BOOLEAN))
      {
         message.putBooleanProperty(key, Boolean.parseBoolean(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_BYTE))
      {
         message.putByteProperty(key, Byte.parseByte(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_BYTES))
      {
         message.putBytesProperty(key, decode(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_DOUBLE))
      {
         message.putDoubleProperty(key, Double.parseDouble(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_FLOAT))
      {
         message.putFloatProperty(key, Float.parseFloat(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_INTEGER))
      {
         message.putIntProperty(key, Integer.parseInt(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_LONG))
      {
         message.putLongProperty(key, Long.parseLong(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_SIMPLE_STRING))
      {
         message.putStringProperty(new SimpleString(key), new SimpleString(value));
      }
      else if (propertyType.equals(XmlDataConstants.PROPERTY_TYPE_STRING))
      {
         message.putStringProperty(key, value);
      }
   }

   private void processMessageBody(Message message) throws XMLStreamException, IOException
   {
      boolean isLarge = false;

      if (reader.getAttributeCount() > 0)
      {
         isLarge = Boolean.parseBoolean(reader.getAttributeValue(0));
      }
      reader.next();
      if (isLarge)
      {
         tempFileName = UUID.randomUUID().toString() + ".tmp";
         log.debug("Creating temp file " + tempFileName + " for large message.");
         OutputStream out = new FileOutputStream(tempFileName);
         while (reader.hasNext())
         {
            if (reader.getEventType() == XMLStreamConstants.END_ELEMENT)
            {
               break;
            }
            else
            {
               String characters = new String(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength());
               String trimmedCharacters = characters.trim();
               if (trimmedCharacters.length() > 0)  // this will skip "indentation" characters
               {
                  byte[] data = decode(trimmedCharacters);
                  out.write(data);
               }
            }
            reader.next();
         }
         out.close();
         FileInputStream fileInputStream = new FileInputStream(tempFileName);
         BufferedInputStream bufferedInput = new BufferedInputStream(fileInputStream);
         ((ClientMessage) message).setBodyInputStream(bufferedInput);
      }
      else
      {
         reader.next(); // step past the "indentation" characters to get to the CDATA with the message body
         String characters = new String(reader.getTextCharacters(), reader.getTextStart(), reader.getTextLength());
         message.getBodyBuffer().writeBytes(decode(characters.trim()));
      }
   }

   private void bindQueue() throws Exception
   {
      String queueName = "";
      String address = "";
      String filter = "";

      for (int i = 0; i < reader.getAttributeCount(); i++)
      {
         String attributeName = reader.getAttributeLocalName(i);
         if (XmlDataConstants.BINDING_ADDRESS.equals(attributeName))
         {
            address = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.BINDING_QUEUE_NAME.equals(attributeName))
         {
            queueName = reader.getAttributeValue(i);
         }
         else if (XmlDataConstants.BINDING_FILTER_STRING.equals(attributeName))
         {
            filter = reader.getAttributeValue(i);
         }
      }

      ClientSession.QueueQuery queueQuery = session.queueQuery(new SimpleString(queueName));

      if (!queueQuery.isExists())
      {
         session.createQueue(address, queueName, filter, true);
         log.debug("Binding queue(name=" + queueName + ", address=" + address + ", filter=" + filter + ")");
      }
      else
      {
         log.debug("Binding " + queueName + " already exists so won't re-bind.");
      }

      addressMap.put(queueName, address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static byte[] decode(String data)
   {
      return Base64.decode(data, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   // Inner classes -------------------------------------------------

}