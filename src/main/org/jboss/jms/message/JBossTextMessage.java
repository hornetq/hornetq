/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;
import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

import org.jboss.logging.Logger;

/**
 * This class implements javax.jms.TextMessage ported from SpyTextMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * 
 * @version $Revision$
 *
 * $Id$
 */
public class JBossTextMessage extends JBossMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 7965361851565655163L;
   
   private static final Logger log = Logger.getLogger(JBossTextMessage.class);

   public static final int TYPE = 5;

   // Attributes ----------------------------------------------------

   protected boolean bodyReadOnly = false;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public JBossTextMessage()
   {
   }

   public JBossTextMessage(String messageID,
                           boolean reliable,
                           long expiration,
                           long timestamp,
                           Map coreHeaders,
                           Serializable payload,
                           String jmsType,
                           int priority,
                           Object correlationID,
                           boolean destinationIsQueue,
                           String destination,
                           boolean replyToIsQueue,
                           String replyTo,
                           Map jmsProperties)
   {
      super(messageID, reliable, expiration, timestamp, coreHeaders, payload,
            jmsType, priority, correlationID, destinationIsQueue, destination, replyToIsQueue,
            replyTo, jmsProperties);
   }

   public JBossTextMessage(JBossTextMessage other)
   {
      super(other);
      this.payload = other.payload;
      this.bodyReadOnly = other.bodyReadOnly;
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS TextMessages.
    */
   protected JBossTextMessage(TextMessage foreign) throws JMSException
   {
      super(foreign);
      String text = foreign.getText();
      if (text != null)
      {
         setText(text);
      }
 
   }

   // Public --------------------------------------------------------

   public int getType()
   {
      return JBossTextMessage.TYPE;
   }

   // TextMessage implementation ------------------------------------

   public void setText(String string) throws JMSException
   {
      if (bodyReadOnly)
         throw new MessageNotWriteableException("Cannot set the content; message is read-only");

      payload = string;
   }

   public String getText() throws JMSException
   {
      return (String)payload;
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      payload = null;
      bodyReadOnly = false;
      super.clearBody();
   }
   
   /** Do any other stuff required to be done after sending the message */
   public void afterSend() throws JMSException
   {      
      super.afterSend();
      
      //Message body must be made read-only
      bodyReadOnly = true;
   }

   // Externalizable implementation ---------------------------------

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      
      if (log.isTraceEnabled()) { log.trace("in readExternal"); }
      
      bodyReadOnly = in.readBoolean();
      
      byte type = in.readByte();
      
      if (log.isTraceEnabled()) { log.trace("type is:" + type); }

      if (type == NULL)
      {
         payload = null;
      }
      else
      {
         payload = in.readUTF();

         /*
          
          TODO
          
          Do we care about this any more?
          Do we support JDK 1.3.x ???
          
          // apply workaround for string > 64K bug in jdk's 1.3.*

          // Read the no. of chunks this message is split into, allocate
          // a StringBuffer that can hold all chunks, read the chunks
          // into the buffer and set 'content' accordingly
          int chunksToRead = in.readInt();
          int bufferSize = chunkSize * chunksToRead;

          // special handling for single chunk
          if (chunksToRead == 1)
          {
          // The text size is likely to be much smaller than the chunkSize
          // so set bufferSize to the min of the input stream available
          // and the maximum buffer size. Since the input stream
          // available() can be <= 0 we check for that and default to
          // a small msg size of 256 bytes.

          int inSize = in.available();
          if (inSize <= 0)
          {
          inSize = 256;
          }

          bufferSize = Math.min(inSize, bufferSize);
          }

          // read off all of the chunks
          StringBuffer sb = new StringBuffer(bufferSize);

          for (int i = 0; i < chunksToRead; i++)
          {
          sb.append(in.readUTF());
          }

          content = sb.toString();
          
          */
      }
   }

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      
      if (log.isTraceEnabled()) { log.trace("in writeExternal"); }
      
      out.writeBoolean(bodyReadOnly);

      if (payload == null)
      {
         out.writeByte(NULL);
      }
      else
      {
         out.write(STRING);
         out.writeUTF((String)payload);

         /*
          
          TODO
          
          Do we care about this any more?
          Do we support JDK1.3.x
          
          // apply workaround for string > 64K bug in jdk's 1.3.*

          // Split content into chunks of size 'chunkSize' and assemble
          // the pieces into a List ...

          // FIXME: could calculate the number of chunks first, then
          //        write as we chunk for efficiency

          ArrayList v = new ArrayList();
          int contentLength = content.length();

          while (contentLength > 0)
          {
          int beginCopy = (v.size()) * chunkSize;
          int endCopy = contentLength <= chunkSize ? beginCopy + contentLength : beginCopy + chunkSize;

          String theChunk = content.substring(beginCopy, endCopy);
          v.add(theChunk);

          contentLength -= chunkSize;
          }

          // Write out the type (OBJECT), the no. of chunks and finally
          // all chunks that have been assembled previously
          out.writeByte(OBJECT);
          out.writeInt(v.size());

          for (int i = 0; i < v.size(); i++)
          {
          out.writeUTF((String) v.get(i));
          }
          */
      }
   }

   // JBossMessage override -----------------------------------------------
   
   public JBossMessage doClone()
   {
      return new JBossTextMessage(this);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   // Public --------------------------------------------------------
}