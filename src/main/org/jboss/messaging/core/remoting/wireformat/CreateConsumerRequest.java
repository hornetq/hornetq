/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import org.jboss.jms.destination.JBossDestination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConsumerRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossDestination destination;
   private final String selector;
   private final boolean noLocal;
   private final String subscriptionName;
   private final boolean connectionConsumer;
   private final boolean autoFlowControl;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConsumerRequest(JBossDestination destination, String selector,
         boolean noLocal, String subscriptionName, boolean connectionConsumer,
         boolean autoFlowControl)
   {
      super(PacketType.REQ_CREATECONSUMER);

      assert destination != null;

      this.destination = destination;
      this.selector = selector;
      this.noLocal = noLocal;
      this.subscriptionName = subscriptionName;
      this.connectionConsumer = connectionConsumer;
      this.autoFlowControl = autoFlowControl;
   }

   // Public --------------------------------------------------------

   public JBossDestination getDestination()
   {
      return destination;
   }

   public String getSelector()
   {
      return selector;
   }

   public boolean isNoLocal()
   {
      return noLocal;
   }

   public String getSubscriptionName()
   {
      return subscriptionName;
   }

   public boolean isConnectionConsumer()
   {
      return connectionConsumer;
   }

   public boolean isAutoFlowControl()
   {
      return autoFlowControl;
   }

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", destination=" + destination);
      buff.append(", selector=" + selector);
      buff.append(", noLocal=" + noLocal);
      buff.append(", subName=" + subscriptionName);
      buff.append(", connectionConsumer=" + connectionConsumer);
      buff.append(", autoFlowControl=" + autoFlowControl);
      buff.append("]");
      return buff.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
