/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Constructed on the client at the time of <code>MessageConsumer</code>
 * creation, this provides a mechanism for a custom developed {@link
 * DeliveryHandler} to retrieve and set client supplied metadata which can
 * inturn drive {@link Consumer} delivery selection.<br/>
 * <br/>
 * During creation this attempts to load a properties file named
 * <code>jboss-jms-consumer-metadata.properties</code> from the client
 * classpath.  This is then serialized and provided to the server-created
 * <code>Consumer</code> which is provided to the {@link DeliveryHandler} during
 * message delivery.  At that time, a custom <code>DeliveryHandler</code> may
 * retrieve this by invoking {@link Consumer#getMetaData()}.  The returned
 * instance may then be used to drive <code>Consumer</code> selection, and or
 * the order of <code>Consumer</code> delivery.<br/>
 * <br/>
 * Additionally, this provides a mechanism to determine the physical ordering of
 * messages during delivery to a specific <code>Consumer</code> to its client.
 * To control this, a client may set the property key
 * <code>org.jboss.jms.server.consumer.metadata.comparator</code> to the fully
 * qualified name of an implementation of {@link Comparator} that handles {@link
 * Message} instances.<br/>
 * <br>
 * In addition to any client specified properties, this also defines serveral
 * methods and attributes for keeping track of consumer performance metrics.
 * They include:<br/>
 * <br/>
 * <b>ActivationTimeMills:</b> The time in milliseconds the consumer has been
 * active.<br/>
 * <br/>
 * <b>AverageDeliveryOperationTimeMillis:</b> Average time in milliseconds
 * physical delivery has taken during the activation lifetime of the owning
 * <code>Consumer</code>.<br/>
 * <br>
 * <b>AverageDeliverySize:</b> Average number of messages delivered in a single
 * delivery operation.<br/>
 * <br>
 * <b>AverageMessageDeliveryOperationTimeMillis:</b> Average time in
 * milliseconds taken to deliver a single message over the activation lifetime
 * of the owning <code>Consumer</code>.<br/>
 * <br>
 * <b>DeliveryOperationCount:</b> Number of times a physical delivery operation
 * has occured.<br/>
 * <br/>
 * <b>LastMessageAcknowledgementTimeMills:</b> Time in milliseconds when the
 * last acknowledgement was processed.<br/>
 * <br>
 * <b>LastMessageDeliveryTimeMillis:</b> Time in milliseconds when the last
 * message was delivered.<br/>
 * <br/>
 * <b>UnacknowledgedMessageCount:</b> Number of messages in the unacknowledged
 * Message queue.<br/>
 * <br/>
 * By default, <code>Consumer</code> instances do not log metrics, and
 * therefore, no standard <code>DeliveryHandler</code> advises them during
 * consumer delivery.  <code>Consumers</code> may be advised to maintain these
 * performance metrics by setting the
 * <code>org.jboss.jms.server.consumer.metadata.statistics.enabled</code>
 * property to <code>true</code> in the server VM.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConsumerMetaData implements Serializable
{
    private Properties customMetaData = null;
    private Comparator messageComparator = null;
    private int messageDeliveryCount = 0;
    private int deliveryOperationCount = 0;
    private int unacknowledgedMessageCount = 0;
    private long lastMessageDeliveryTimeMillis = 0;
    private long lastMessageAcknowledgementTimeMills = 0;
    private int averageDeliverySize = 0;
    private long averageDeliveryOperationTimeMillis = 0;
    private long activationTimeMillis = 0;

    public ConsumerMetaData()
    {
        this.loadCustomMetaData();
    }

    public static boolean isEnabled()
    {
        return Boolean.getBoolean(
                "org.jboss.jms.server.consumer.metadata.statistics.enabled");
    }

    synchronized Comparator getMessageComparator()
    {
        if (this.messageComparator == null)
        {
            if (this.hasCustomMetaData())
            {
                String messageComparatorName =
                        this.customMetaData.getProperty(
                                "org.jboss.jms.server.consumer.metatdata.comparator");
                if (messageComparatorName != null)
                {
                    try
                    {
                        Class messageComparatorClass =
                                Thread
                                .currentThread()
                                .getContextClassLoader()
                                .loadClass(
                                        messageComparatorName);
                        Comparator messageComparator =
                                (Comparator) messageComparatorClass.newInstance();
                        this.messageComparator =
                                new MessageReferenceComparator(messageComparator);
                    }
                    catch (Exception exception)
                    {
                        this.messageComparator =
                                new MessageReferenceComparator(
                                        new StandardMessageComparator());
                    }
                }
                this.messageComparator =
                        new MessageReferenceComparator(
                                new StandardMessageComparator());
            }
            this.messageComparator =
                    new MessageReferenceComparator(new StandardMessageComparator());
        }
        return this.messageComparator;
    }

    public int getMessageDeliveryCount()
    {
        return this.messageDeliveryCount;
    }

    void incrementMessageDeliveryCount()
    {
        this.messageDeliveryCount++;
    }

    void addMessageDeliveryCount(int value)
    {
        this.messageDeliveryCount = this.messageDeliveryCount + value;
    }

    public int getDeliveryOperationCount()
    {
        return this.deliveryOperationCount;
    }

    void incrementDeliveryOperationCount()
    {
        this.deliveryOperationCount++;
    }

    public int getUnacknowledgedMessageCount()
    {
        return this.unacknowledgedMessageCount;
    }

    void incrementUnacknowledgedMessageCount()
    {
        this.unacknowledgedMessageCount++;
    }

    void decrementUnacknowledgedMessageCount()
    {
        this.unacknowledgedMessageCount--;
    }

    public long getLastMessageDeliveryTimeMillis()
    {
        return this.lastMessageDeliveryTimeMillis;
    }

    void setLastMessageDeliveryTimeMillis(long lastMessageDeliveryTimeMillis)
    {
        this.lastMessageDeliveryTimeMillis = lastMessageDeliveryTimeMillis;
    }

    public long getLastMessageAcknowledgementTimeMills()
    {
        return this.lastMessageAcknowledgementTimeMills;
    }

    void setLastMessageAcknowledgementTimeMills(long lastMessageAcknowledgementTimeMills)
    {
        this.lastMessageAcknowledgementTimeMills =
                lastMessageAcknowledgementTimeMills;
    }

    public int getAverageDeliverySize()
    {
        return this.averageDeliverySize;
    }

    void reaverageDeliverySize(int deliverySize)
    {
        if (this.deliveryOperationCount != 0)
        {
            this.averageDeliverySize =
                    this.averageDeliverySize
                    + deliverySize / this.deliveryOperationCount;
        }
    }

    public long getAverageDeliveryOperationTimeMillis()
    {
        return this.averageDeliveryOperationTimeMillis;
    }

    void reaverageDeliveryOperationTimeMillis(long deliveryOperationTime)
    {
        if (this.deliveryOperationCount != 0)
        {
            this.averageDeliveryOperationTimeMillis =
                    this.averageDeliveryOperationTimeMillis
                    + deliveryOperationTime / this.deliveryOperationCount;
        }
    }

    public long getAverageMessageDeliveryOperationTimeMillis()
    {
        if (this.averageDeliverySize == 0)
        {
            return 0;
        }
        return this.averageDeliveryOperationTimeMillis
                / this.averageDeliverySize;
    }

    public long getActivationLifeTimeMills()
    {
        return System.currentTimeMillis() - this.activationTimeMillis;
    }

    void setActivationTimeMills(long activationTime)
    {
        this.activationTimeMillis = activationTime;
    }

    public boolean hasCustomMetaData()
    {
        return (this.customMetaData != null)
                && (this.customMetaData.isEmpty() == false);
    }

    public String getCustomMetaData(String key)
    {
        if (this.hasCustomMetaData())
        {
            return this.customMetaData.getProperty(key);
        }
        else
        {
            return null;
        }
    }

    public synchronized void setCustomMetaData(String key, String value)
    {
        if (!this.hasCustomMetaData())
        {
            this.customMetaData = new Properties();
        }
        this.customMetaData.setProperty(key, value);
    }

    public String toString()
    {

        String metaDataString = "CONSUMER METADATA";
        if (isEnabled())
        {
            metaDataString =
                    metaDataString =
                    "\n\nSTATISTICS:\n"
                    + "Consumer Activated:                   "
                    + this.activationTimeMillis
                    + "\n"
                    + "Total Number of messages delivered:   "
                    + this.messageDeliveryCount
                    + "\n"
                    + "Total Number of delivery operations:  "
                    + this.deliveryOperationCount
                    + "\n"
                    + "Average Number of Messages/Delivery:  "
                    + this.averageDeliverySize
                    + "\n"
                    + "Averge delivery Operation Time Mills: "
                    + this.averageDeliveryOperationTimeMillis
                    + "\n"
                    + "Average Time Mills/Message:           "
                    + this.getAverageMessageDeliveryOperationTimeMillis();
        }
        if (this.hasCustomMetaData())
        {
            metaDataString = metaDataString + "\n\nCUSTOM METADATA\n";
            Enumeration properties = this.customMetaData.keys();
            while (properties.hasMoreElements())
            {
                String key = (String) properties.nextElement();
                metaDataString =
                        metaDataString
                        + key
                        + ": "
                        + this.customMetaData.getProperty(key)
                        + "\n";
            }
        }
        return metaDataString;
    }

    private void loadCustomMetaData()
    {
        this.customMetaData = new Properties();
        try
        {
            this.customMetaData.load(
                    Thread
                    .currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(
                            "jboss-jms-consumer-metadata.properties"));
        }
        catch (Exception exception)
        {
            this.customMetaData = null;
        }
    }

    private class MessageReferenceComparator implements Comparator
    {
        private Comparator delegateComparator = null;

        public MessageReferenceComparator(Comparator delegateComparator)
        {
            this.delegateComparator = delegateComparator;
        }

        public int compare(Object firstObject, Object secondObject)
        {
            MessageReference firstMessageReference =
                    (MessageReference) firstObject;
            MessageReference secondMessageReference =
                    (MessageReference) secondObject;
            if (firstMessageReference.equals(secondMessageReference))
            {
                return 0;
            }
            return this.delegateComparator.compare(
                    firstMessageReference.getMessage(),
                    secondMessageReference.getMessage());

        }
    }

    private class StandardMessageComparator implements Comparator
    {
        public int compare(Object firstObject, Object secondObject)
        {
            Message firstMessage = (Message) firstObject;
            Message secondMessage = (Message) secondObject;

            int value = this.comparePriority(firstMessage, secondMessage);

            if (value == 0)
            {
                value = this.compareGroupSequence(firstMessage, secondMessage);
            }
            if (value == 0)
            {
                value = 1;
            }

            return value;
        }

        private int comparePriority(
                Message firstMessage,
                Message secondMessage)
        {
            int firstMessagePriority = Message.DEFAULT_PRIORITY;
            int secondMessagePriority = Message.DEFAULT_PRIORITY;
            try
            {
                firstMessagePriority = firstMessage.getJMSPriority();
                secondMessagePriority = secondMessage.getJMSPriority();
            }
            catch (JMSException exception)
            {
                return 0;
            }
            if (firstMessagePriority > secondMessagePriority)
            {
                return -1;
            }
            else if (firstMessagePriority < secondMessagePriority)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }

        private int compareGroupSequence(
                Message firstMessage,
                Message secondMessage)
        {
            int firstMessageGroupSequenceNumber = 0;
            int secondMessageGroupSequenceNumber = 0;
            try
            {
                String firstMessageGroupId, secondMessageGroupId;
                firstMessageGroupId =
                        firstMessage.getStringProperty("JMXSGroupID");
                secondMessageGroupId =
                        secondMessage.getStringProperty("JMXSGroupID");
                if (firstMessageGroupId != null
                        && secondMessageGroupId == null)
                {
                    firstMessageGroupSequenceNumber =
                            firstMessage.getIntProperty("JMSXGroupSeq");
                    secondMessageGroupSequenceNumber =
                            secondMessage.getIntProperty("JMSXGroupSeq");
                }
            }
            catch (Exception exception)
            {
                return 0;
            }
            if (firstMessageGroupSequenceNumber
                    > secondMessageGroupSequenceNumber)
            {
                return 1;
            }
            else if (
                    firstMessageGroupSequenceNumber
                    < firstMessageGroupSequenceNumber)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }
    }
}