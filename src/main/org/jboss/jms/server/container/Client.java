/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.server.BrowserEndpointFactory;
import org.jboss.jms.server.DeliveryEndpointFactory;
import org.jboss.jms.server.MessageBroker;
import org.jboss.util.id.GUID;

/**
 * The serverside representation of the client
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class Client
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The Message broker */
   private MessageBroker broker;

   // Static --------------------------------------------------------

   public static Client getClient(Invocation invocation)
   {
      return (Client) invocation.getMetaData("JMS", "Client");
   }

   // Constructors --------------------------------------------------

   public Client(MessageBroker broker)
   {
      this.broker = broker;
   }

   // Public --------------------------------------------------------

   public SimpleMetaData createSession(MethodInvocation invocation)
   {
      return getMetaData();
   }

   public SimpleMetaData createBrowser(MethodInvocation invocation)
   {
      SimpleMetaData result = getMetaData();
      
      JBossDestination destination = (JBossDestination) invocation.getArguments()[0];
      String selector = (String) invocation.getArguments()[1];
      BrowserEndpointFactory endpointFactory = broker.getBrowserEndpointFactory(destination, selector);
      result.addMetaData("JMS", "BrowserEndpointFactory", endpointFactory);
      return result;
   }

   public SimpleMetaData createConsumer(MethodInvocation invocation)
   {
      return getMetaData();
   }

   public SimpleMetaData createProducer(MethodInvocation invocation)
   {
      SimpleMetaData result = getMetaData();
      
      JBossDestination destination = (JBossDestination) invocation.getArguments()[0];
      DeliveryEndpointFactory endpointFactory = broker.getDeliveryEndpointFactory(destination);
      result.addMetaData("JMS", "DeliveryEndpointFactory", endpointFactory);
      return result;
   }

   public SimpleMetaData getMetaData()
   {
      SimpleMetaData result = new SimpleMetaData();
      result.addMetaData("JMS", "Client", this);
      result.addMetaData("JMS", "OID", GUID.asString());
      return result;
   }

   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
