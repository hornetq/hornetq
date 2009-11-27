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

package org.hornetq.jms.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.PropertyConversionException;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.jms.HornetQDestination;
import org.hornetq.utils.SimpleString;

/**
 * Implementation of a JMS Message JMS Messages only live on the client side - the server only deals with MessageImpl
 * instances
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:bershath@yahoo.com">Tyronne Wickramarathne</a> Partially ported from JBossMQ implementation
 *         originally written by:
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author Hiram Chirino (Cojonudo14@hotmail.com)
 * @author David Maplesden (David.Maplesden@orion.co.nz)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a> 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a> 
 * $Id: HornetQRAMessage.java 3466 2007-12-10 18:44:52Z timfox $
 */
public class HornetQMessage implements javax.jms.Message
{
   // Constants -----------------------------------------------------

   public static final SimpleString REPLYTO_HEADER_NAME = ClientMessageImpl.REPLYTO_HEADER_NAME;

   public static final SimpleString CORRELATIONID_HEADER_NAME = new SimpleString("JMSCorrelationID");

   public static final SimpleString HORNETQ_MESSAGE_ID = new SimpleString("JMSMessageID");

   public static final SimpleString TYPE_HEADER_NAME = new SimpleString("JMSType");

   private static final SimpleString JMS = new SimpleString("JMS");

   private static final SimpleString JMSX = new SimpleString("JMSX");

   private static final SimpleString JMS_ = new SimpleString("JMS_");

   public static final String JMSXDELIVERYCOUNT = "JMSXDeliveryCount";

   public static final String JMS_HORNETQ_INPUT_STREAM = "JMS_HQ_InputStream";

   public static final String JMS_HORNETQ_OUTPUT_STREAM = "JMS_HQ_OutputStream";

   public static final String JMS_HORNETQ_SAVE_STREAM = "JMS_HQ_SaveStream";

   public static final String JMSXGROUPID = "JMSXGroupID";

   // Used when bridging a message
   public static final String JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST = "HQ_BRIDGE_MSG_ID_LIST";

   public static final byte TYPE = 0;

   public static Map<String, Object> coreMaptoJMSMap(final Map<String, Object> coreMessage)
   {
      Map<String, Object> jmsMessage = new HashMap<String, Object>();

      String deliveryMode = (Boolean)coreMessage.get("durable") ? "PERSISTENT" : "NON_PERSISTENT";
      byte priority = (Byte)coreMessage.get("priority");
      long timestamp = (Long)coreMessage.get("timestamp");
      long expiration = (Long)coreMessage.get("expiration");

      jmsMessage.put("JMSPriority", priority);
      jmsMessage.put("JMSTimestamp", timestamp);
      jmsMessage.put("JMSExpiration", expiration);
      jmsMessage.put("JMSDeliveryMode", deliveryMode);

      for (Map.Entry<String, Object> entry : coreMessage.entrySet())
      {
         if (entry.getKey().equals("messageID") || entry.getKey().equals("destination") ||
             entry.getKey().equals("type") ||
             entry.getKey().equals("durable") ||
             entry.getKey().equals("expiration") ||
             entry.getKey().equals("timestamp") ||
             entry.getKey().equals("priority"))
         {
            // Ignore
         }
         else
         {
            Object value = entry.getValue();
            if (value instanceof SimpleString)
            {
               jmsMessage.put(entry.getKey(), value.toString());
            }
            else
            {
               jmsMessage.put(entry.getKey(), value);
            }
         }
      }

      return jmsMessage;
   }

   // Static --------------------------------------------------------

   private static final HashSet<String> reservedIdentifiers = new HashSet<String>();
   static
   {
      reservedIdentifiers.add("NULL");
      reservedIdentifiers.add("TRUE");
      reservedIdentifiers.add("FALSE");
      reservedIdentifiers.add("NOT");
      reservedIdentifiers.add("AND");
      reservedIdentifiers.add("OR");
      reservedIdentifiers.add("BETWEEN");
      reservedIdentifiers.add("LIKE");
      reservedIdentifiers.add("IN");
      reservedIdentifiers.add("IS");
      reservedIdentifiers.add("ESCAPE");
   }

   private static final Logger log = Logger.getLogger(HornetQMessage.class);

   public static HornetQMessage createMessage(final ClientMessage message, final ClientSession session)
   {
      int type = message.getType();

      HornetQMessage msg;

      switch (type)
      {
         case HornetQMessage.TYPE: // 0
         {
            msg = new HornetQMessage(message, session);
            break;
         }
         case HornetQBytesMessage.TYPE: // 4
         {
            msg = new HornetQBytesMessage(message, session);
            break;
         }
         case HornetQMapMessage.TYPE: // 5
         {
            msg = new HornetQMapMessage(message, session);
            break;
         }
         case HornetQObjectMessage.TYPE:
         {
            msg = new HornetQObjectMessage(message, session);
            break;
         }
         case HornetQStreamMessage.TYPE: // 6
         {
            msg = new HornetQStreamMessage(message, session);
            break;
         }
         case HornetQTextMessage.TYPE: // 3
         {
            msg = new HornetQTextMessage(message, session);
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid message type " + type);
         }
      }

      return msg;
   }

   // Attributes ----------------------------------------------------

   // The underlying message
   protected ClientMessage message;

   private ClientSession session;

   // Read-only?
   protected boolean readOnly;

   // Properties read-only?
   protected boolean propertiesReadOnly;

   // Cache it
   private Destination dest;

   // Cache it
   private String msgID;

   // Cache it
   private Destination replyTo;

   // Cache it
   private String jmsCorrelationID;

   // Cache it
   private String jmsType;

   // Constructors --------------------------------------------------
   
   /*
    * Create a new message prior to sending
    */
   protected HornetQMessage(final byte type, final ClientSession session)
   {
      message = session.createClientMessage(type, true, 0, System.currentTimeMillis(), (byte)4);

   }

   public HornetQMessage(final ClientSession session)
   {
      this(HornetQMessage.TYPE, session);
   }

   /**
    * Constructor for when receiving a message from the server
    */
   public HornetQMessage(final ClientMessage message, final ClientSession session)
   {
      this.message = message;

      readOnly = true;

      propertiesReadOnly = true;

      this.session = session;
   }

   /*
    * A constructor that takes a foreign message
    */
   public HornetQMessage(final Message foreign, final ClientSession session) throws JMSException
   {
      this(foreign, HornetQMessage.TYPE, session);
   }
   
   public HornetQMessage()
   {
   }

   protected HornetQMessage(final Message foreign, final byte type, final ClientSession session) throws JMSException
   {
      this(type, session);

      setJMSTimestamp(foreign.getJMSTimestamp());

      try
      {
         byte[] corrIDBytes = foreign.getJMSCorrelationIDAsBytes();
         setJMSCorrelationIDAsBytes(corrIDBytes);
      }
      catch (JMSException e)
      {
         // specified as String
         String corrIDString = foreign.getJMSCorrelationID();
         if (corrIDString != null)
         {
            setJMSCorrelationID(corrIDString);
         }
      }

      setJMSReplyTo(foreign.getJMSReplyTo());
      setJMSDestination(foreign.getJMSDestination());
      setJMSDeliveryMode(foreign.getJMSDeliveryMode());
      setJMSExpiration(foreign.getJMSExpiration());
      setJMSPriority(foreign.getJMSPriority());
      setJMSType(foreign.getJMSType());

      // We can't avoid a cast warning here since getPropertyNames() is on the JMS API
      for (Enumeration<String> props = foreign.getPropertyNames(); props.hasMoreElements();)
      {
         String name = props.nextElement();

         Object prop = foreign.getObjectProperty(name);

         setObjectProperty(name, prop);
      }
   }

   // javax.jmx.Message implementation ------------------------------

   public String getJMSMessageID()
   {
      if (msgID == null)
      {
         SimpleString id = message.getSimpleStringProperty(HORNETQ_MESSAGE_ID);

         msgID = id == null ? null : id.toString();
      }
      return msgID;
   }

   public void setJMSMessageID(final String jmsMessageID) throws JMSException
   {
      if (jmsMessageID != null && !jmsMessageID.startsWith("ID:"))
      {
         throw new JMSException("JMSMessageID must start with ID:");
      }
      
      if (jmsMessageID == null)
      {
         message.removeProperty(HORNETQ_MESSAGE_ID);
      }
      else
      {
         message.putStringProperty(HORNETQ_MESSAGE_ID, new SimpleString(jmsMessageID));
      }
      
      msgID = jmsMessageID;
   }
   
   public long getJMSTimestamp() throws JMSException
   {
      return message.getTimestamp();
   }

   public void setJMSTimestamp(final long timestamp) throws JMSException
   {
      message.setTimestamp(timestamp);
   }

   public byte[] getJMSCorrelationIDAsBytes() throws JMSException
   {
      Object obj = message.getObjectProperty(CORRELATIONID_HEADER_NAME);

      if (obj instanceof byte[])
      {
         return (byte[])obj;
      }
      else
      {
         return null;
      }
   }

   public void setJMSCorrelationIDAsBytes(final byte[] correlationID) throws JMSException
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSException("Please specify a non-zero length byte[]");
      }
      message.putBytesProperty(CORRELATIONID_HEADER_NAME, correlationID);
   }

   public void setJMSCorrelationID(final String correlationID) throws JMSException
   {
      if (correlationID == null)
      {
         message.removeProperty(CORRELATIONID_HEADER_NAME);

         jmsCorrelationID = null;
      }
      else
      {
         message.putStringProperty(CORRELATIONID_HEADER_NAME, new SimpleString(correlationID));

         jmsCorrelationID = correlationID;
      }
   }

   public String getJMSCorrelationID() throws JMSException
   {
      if (jmsCorrelationID == null)
      {
         jmsCorrelationID = message.getStringProperty(CORRELATIONID_HEADER_NAME);
      }

      return jmsCorrelationID;
   }

   public Destination getJMSReplyTo() throws JMSException
   {
      if (replyTo == null)
      {
         SimpleString repl = message.getSimpleStringProperty(REPLYTO_HEADER_NAME);

         if (repl != null)
         {
            replyTo = HornetQDestination.fromAddress(repl.toString());
         }
      }
      return replyTo;
   }

   public void setJMSReplyTo(final Destination dest) throws JMSException
   {
      if (dest == null)
      {
         message.removeProperty(REPLYTO_HEADER_NAME);

         replyTo = null;
      }
      else
      {
         if (dest instanceof HornetQDestination == false)
         {
            throw new InvalidDestinationException("Not a JBoss destination " + dest);
         }

         HornetQDestination jbd = (HornetQDestination)dest;

         message.putStringProperty(REPLYTO_HEADER_NAME, jbd.getSimpleAddress());

         replyTo = jbd;
      }
   }

   public Destination getJMSDestination() throws JMSException
   {
      if (dest == null)
      {
         SimpleString sdest = message.getDestination();

         dest = sdest == null ? null : HornetQDestination.fromAddress(sdest.toString());
      }

      return dest;
   }

   public void setJMSDestination(final Destination destination) throws JMSException
   {
      dest = destination;
   }

   public int getJMSDeliveryMode() throws JMSException
   {
      return message.isDurable() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
   }

   public void setJMSDeliveryMode(final int deliveryMode) throws JMSException
   {
      if (deliveryMode == DeliveryMode.PERSISTENT)
      {
         message.setDurable(true);
      }
      else if (deliveryMode == DeliveryMode.NON_PERSISTENT)
      {
         message.setDurable(false);
      }
      else
      {
         throw new JMSException("DeliveryImpl mode must be either DeliveryMode.PERSISTENT " + "or DeliveryMode.NON_PERSISTENT");
      }
   }

   public boolean getJMSRedelivered() throws JMSException
   {
      return message.getDeliveryCount() > 1;
   }

   public void setJMSRedelivered(final boolean redelivered) throws JMSException
   {
      if (!redelivered)
      {
         message.setDeliveryCount(1);
      }
      else
      {
         if (message.getDeliveryCount() > 1)
         {
            // do nothing
         }
         else
         {
            message.setDeliveryCount(2);
         }
      }
   }

   public void setJMSType(final String type) throws JMSException
   {
      if (type != null)
      {
         message.putStringProperty(TYPE_HEADER_NAME, new SimpleString(type));

         jmsType = type;
      }
   }

   public String getJMSType() throws JMSException
   {
      if (jmsType == null)
      {
         SimpleString ss = message.getSimpleStringProperty(TYPE_HEADER_NAME);

         if (ss != null)
         {
            jmsType = ss.toString();
         }
      }
      return jmsType;
   }

   public long getJMSExpiration() throws JMSException
   {
      return message.getExpiration();
   }

   public void setJMSExpiration(final long expiration) throws JMSException
   {
      message.setExpiration(expiration);
   }

   public int getJMSPriority() throws JMSException
   {
      return message.getPriority();
   }

   public void setJMSPriority(final int priority) throws JMSException
   {
      checkPriority(priority);

      message.setPriority((byte)priority);
   }

   public void clearProperties() throws JMSException
   {
      List<SimpleString> toRemove = new ArrayList<SimpleString>();

      for (SimpleString propName : message.getPropertyNames())
      {
         if (!propName.startsWith(JMS) || propName.startsWith(JMSX) || propName.startsWith(JMS_))
         {
            toRemove.add(propName);
         }
      }

      for (SimpleString propName : toRemove)
      {
         message.removeProperty(propName);
      }

      propertiesReadOnly = false;
   }

   public void clearBody() throws JMSException
   {
      readOnly = false;
   }

   public boolean propertyExists(final String name) throws JMSException
   {
      return message.containsProperty(new SimpleString(name)) || name.equals(JMSXDELIVERYCOUNT) ||
             JMSXGROUPID.equals(name) &&
             message.containsProperty(MessageImpl.HDR_GROUP_ID);
   }

   public boolean getBooleanProperty(final String name) throws JMSException
   {
      try
      {
         return message.getBooleanProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public byte getByteProperty(final String name) throws JMSException
   {
      try
      {
         return message.getByteProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public short getShortProperty(final String name) throws JMSException
   {
      try
      {
         return message.getShortProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public int getIntProperty(final String name) throws JMSException
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return message.getDeliveryCount();
      }

      try
      {
         return message.getIntProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public long getLongProperty(final String name) throws JMSException
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return message.getDeliveryCount();
      }

      try
      {
         return message.getLongProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public float getFloatProperty(final String name) throws JMSException
   {
      try
      {
         return message.getFloatProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public double getDoubleProperty(final String name) throws JMSException
   {
      try
      {
         return message.getDoubleProperty(new SimpleString(name));
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public String getStringProperty(final String name) throws JMSException
   {
      if (JMSXDELIVERYCOUNT.equals(name))
      {
         return String.valueOf(message.getDeliveryCount());
      }

      try
      {
         if (JMSXGROUPID.equals(name))
         {
            return message.getStringProperty(MessageImpl.HDR_GROUP_ID);
         }
         else
         {
            return message.getStringProperty(new SimpleString(name));
         }
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public Object getObjectProperty(final String name) throws JMSException
   {
      if (JMS_HORNETQ_INPUT_STREAM.equals(name))
      {
         return message.getBodyInputStream();
      }
      else if (JMSXDELIVERYCOUNT.equals(name))
      {
         return String.valueOf(message.getDeliveryCount());
      }

      Object val = message.getObjectProperty(name);
      if (val instanceof SimpleString)
      {
         val = ((SimpleString)val).toString();
      }
      return val;
   }

   public Enumeration getPropertyNames() throws JMSException
   {
      HashSet<String> set = new HashSet<String>();

      for (SimpleString propName : message.getPropertyNames())
      {
         if (!propName.startsWith(JMS) || propName.startsWith(JMSX) || propName.startsWith(JMS_))
         {
            set.add(propName.toString());
         }
      }

      set.add(JMSXDELIVERYCOUNT);

      return Collections.enumeration(set);
   }

   public void setBooleanProperty(final String name, final boolean value) throws JMSException
   {
      checkProperty(name, value);
      
      message.putBooleanProperty(new SimpleString(name), value);
   }

   public void setByteProperty(final String name, final byte value) throws JMSException
   {
      checkProperty(name, value);
      message.putByteProperty(new SimpleString(name), value);
   }

   public void setShortProperty(final String name, final short value) throws JMSException
   {
      checkProperty(name, value);
      message.putShortProperty(new SimpleString(name), value);
   }

   public void setIntProperty(final String name, final int value) throws JMSException
   {
      checkProperty(name, value);
      message.putIntProperty(new SimpleString(name), value);
   }

   public void setLongProperty(final String name, final long value) throws JMSException
   {
      checkProperty(name, value);
      message.putLongProperty(new SimpleString(name), value);
   }

   public void setFloatProperty(final String name, final float value) throws JMSException
   {
      checkProperty(name, value);
      message.putFloatProperty(new SimpleString(name), value);
   }

   public void setDoubleProperty(final String name, final double value) throws JMSException
   {
      checkProperty(name, value);
      message.putDoubleProperty(new SimpleString(name), value);
   }

   public void setStringProperty(final String name, final String value) throws JMSException
   {
      checkProperty(name, value);

      if (value == null)
      {
         // This is ok - when we try to read the same key it will return null too
         return;
      }

      if (JMSXGROUPID.equals(name))
      {
         message.putStringProperty(MessageImpl.HDR_GROUP_ID, new SimpleString(value));
      }
      else
      {
         message.putStringProperty(new SimpleString(name), new SimpleString(value));
      }
   }

   public void setObjectProperty(final String name, final Object value) throws JMSException
   {
      if (JMS_HORNETQ_OUTPUT_STREAM.equals(name))
      {
         setOutputStream((OutputStream)value);

         return;
      }
      else if (JMS_HORNETQ_SAVE_STREAM.equals(name))
      {
         saveToOutputStream((OutputStream)value);

         return;
      }

      checkProperty(name, value);

      if (value == null)
      {
         // This is ok - when we try to read the same key it will return null too
         return;
      }

      if (JMS_HORNETQ_INPUT_STREAM.equals(name))
      {
         setInputStream((InputStream)value);

         return;
      }

      try
      {
         message.putObjectProperty(new SimpleString(name), value);
      }
      catch (PropertyConversionException e)
      {
         throw new MessageFormatException(e.getMessage());
      }
   }

   public void acknowledge() throws JMSException
   {
      if (session != null)
      {
         try
         {
            session.commit();
         }
         catch (HornetQException e)
         {
            throw JMSExceptionHelper.convertFromHornetQException(e);
         }
      }
   }

   // Public --------------------------------------------------------

   public void resetMessageID(String msgID)
   {
      this.msgID = msgID;
   }
   
   public ClientMessage getCoreMessage()
   {
      return message;
   }

   public void doBeforeSend() throws Exception
   {
      message.getBodyBuffer().resetReaderIndex();
   }

   public void doBeforeReceive() throws Exception
   {
      HornetQBuffer body = message.getBodyBuffer();
      
      if (body != null)
      {
         body.resetReaderIndex();
      }
   }

   public byte getType()
   {
      return HornetQMessage.TYPE;
   }

   public void setInputStream(final InputStream input) throws JMSException
   {
      checkStream();
      if (readOnly)
      {
         throw new MessageNotWriteableException("Message is read-only");
      }

      message.setBodyInputStream(input);
   }

   public void setOutputStream(final OutputStream output) throws JMSException
   {
      checkStream();
      if (!readOnly)
      {
         throw new IllegalStateException("OutputStream property is only valid on received messages");
      }

      try
      {
         message.setOutputStream(output);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public void saveToOutputStream(final OutputStream output) throws JMSException
   {
      checkStream();
      if (!readOnly)
      {
         throw new IllegalStateException("OutputStream property is only valid on received messages");
      }

      try
      {
         message.saveToOutputStream(output);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public boolean waitCompletionOnStream(final long timeWait) throws JMSException
   {
      checkStream();
      try
      {
         return message.waitOutputStreamCompletion(timeWait);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   @Override
   public String toString()
   {
      StringBuffer sb = new StringBuffer("HornetQMessage[");
      sb.append(getJMSMessageID());
      sb.append("]:");
      sb.append(message.isDurable() ? "PERSISTENT" : "NON-PERSISTENT");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void checkWrite() throws JMSException
   {
      if (readOnly)
      {
         throw new MessageNotWriteableException("Message is read-only");
      }
   }

   protected void checkRead() throws JMSException
   {
      if (!readOnly)
      {
         throw new MessageNotReadableException("Message is write-only");
      }
   }

   // Private ------------------------------------------------------------

   private void checkStream() throws JMSException
   {
      if (!(message.getType() == HornetQBytesMessage.TYPE || message.getType() == HornetQStreamMessage.TYPE))
      {
         throw new IllegalStateException("LargeMessage streaming is only possible on ByteMessage or StreamMessage");
      }
   }

   private void checkProperty(final String name, final Object value) throws JMSException
   {
      if (propertiesReadOnly)
      {
         if (name.equals(JMS_HORNETQ_INPUT_STREAM))
         {
            throw new MessageNotWriteableException("You cannot set the Input Stream on received messages. Did you mean " + JMS_HORNETQ_OUTPUT_STREAM +
                                                   " or " +
                                                   JMS_HORNETQ_SAVE_STREAM +
                                                   "?");
         }
         else
         {
            throw new MessageNotWriteableException("Message is read-only");
         }
      }

      if (name == null)
      {
         throw new IllegalArgumentException("The name of a property must not be null.");
      }

      if (name.equals(""))
      {
         throw new IllegalArgumentException("The name of a property must not be an empty String.");
      }

      if (!isValidJavaIdentifier(name))
      {
         throw new IllegalArgumentException("The property name '" + name + "' is not a valid java identifier.");
      }

      if (reservedIdentifiers.contains(name))
      {
         throw new IllegalArgumentException("The property name '" + name + "' is reserved due to selector syntax.");
      }

      if (name.startsWith("JMS"))
      {
         if (name.length() > 3)
         {
            char c = name.charAt(3);
            if (c != 'X' && c != '_')
            {
               // See http://java.sun.com/javaee/5/docs/api/
               // (java.jms.Message javadoc)
               // "Property names must obey the rules for a message selector identifier"
               // "Any name that does not begin with 'JMS' is an application-specific property name"
               throw new IllegalArgumentException("The property name '" + name +
                                                  "' is illegal since it starts with JMS");
            }
         }
         else
         {
            throw new IllegalArgumentException("The property name '" + name + "' is illegal since it starts with JMS");
         }
      }
   }

   private boolean isValidJavaIdentifier(final String s)
   {
      if (s == null || s.length() == 0)
      {
         return false;
      }

      char[] c = s.toCharArray();

      if (!Character.isJavaIdentifierStart(c[0]))
      {
         return false;
      }

      for (int i = 1; i < c.length; i++)
      {
         if (!Character.isJavaIdentifierPart(c[i]))
         {
            return false;
         }
      }

      return true;
   }

   private void checkPriority(final int priority) throws JMSException
   {
      if (priority < 0 || priority > 9)
      {
         throw new JMSException(priority + " is not valid: priority must be between 0 and 9");
      }
   }

   // Inner classes -------------------------------------------------
}
