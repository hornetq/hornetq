/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

import java.util.StringTokenizer;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.messaging.core.contract.MessagingComponent;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

/**
 * The base of a JBoss Messaging destination service. Both deployed or programatically created
 * destinations will eventually get one of these.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * 
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DestinationServiceSupport extends ServiceMBeanSupport implements DestinationMBean
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ObjectName serverPeerObjectName;
   
   private ObjectName dlqObjectName;
   
   private ObjectName expiryQueueObjectName;
   
   protected boolean started = false;
   
   protected ManagedDestination destination;
   
   protected ServerPeer serverPeer;
   
   protected int nodeId;
    
   private boolean createdProgrammatically;
   
   
   // Constructors --------------------------------------------------
   
   public DestinationServiceSupport(boolean createdProgrammatically)
   {
      this.createdProgrammatically = createdProgrammatically;     
   }
   
   public DestinationServiceSupport()
   {
   }
   
   // ServerPlugin implementation ------------------------------------------
   
   public MessagingComponent getInstance()
   {
      return destination;
   }

   // ServiceMBeanSupport overrides -----------------------------------
   
   public synchronized void startService() throws Exception
   {
      super.startService();
      
      try
      {
         serverPeer = (ServerPeer)server.getAttribute(serverPeerObjectName, "Instance");
               	      
         destination.setServerPeer(serverPeer);
               	
         nodeId = serverPeer.getServerPeerID();
         
         String name = null;
                  
         if (serviceName != null)
         {
            name = serviceName.getKeyProperty("name");
         }
   
         if (name == null || name.length() == 0)
         {
            throw new IllegalStateException( "The " + (isQueue() ? "queue" : "topic") + " " +
                                             "name was not properly set in the service's" +
                                             "ObjectName");
         }                  
         
         destination.setName(name);         
         
         // http://jira.jboss.com/jira/browse/JBMESSAGING-976
         if (destination.getSecurityConfig() != null)
         {
         	serverPeer.getSecurityManager().setSecurityConfig(isQueue(), destination.getName(), destination.getSecurityConfig());
         }
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " startService");
      }     
   }
   
   public synchronized void stopService() throws Exception
   {
      super.stopService();    
   }
   
   // JMX managed attributes ----------------------------------------
   
   public String getName()
   {
      return destination.getName();
   }
   
   public String getJNDIName()
   {
      return destination.getJndiName();
   }

   public void setJNDIName(String jndiName) throws Exception
   {
      try
      {
         if (started)
         {
            log.warn("Cannot change the value of the JNDI name after initialization!");
            return;
         }
   
         destination.setJndiName(jndiName);      
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " setJNDIName");
      }
   }

   public void setServerPeer(ObjectName on)
   {
      if (started)
      {
         log.warn("Cannot change the value of associated " +
                  "server's ObjectName after initialization!");
         return;
      }

      serverPeerObjectName = on;
   }

   public ObjectName getServerPeer()
   {
      return serverPeerObjectName;
   }
   
   public void setDLQ(ObjectName on) throws Exception
   {
      dlqObjectName = on;
      
      ManagedQueue dest = null;
      
      try
      {
         
         try
         {         
            dest = (ManagedQueue)getServer().
               getAttribute(dlqObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }

         destination.setDLQ(dest);       
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, " setDLQ");
      }
   }
   
   public ObjectName getDLQ()
   {
      return dlqObjectName;
   }
   
   public void setExpiryQueue(ObjectName on) throws Exception
   {
      expiryQueueObjectName = on;
      
      ManagedQueue dest = null;
      
      try
      {         
         try
         {         
            dest = (ManagedQueue)getServer().
               getAttribute(expiryQueueObjectName, "Instance");
         }
         catch (InstanceNotFoundException e)
         {
            //Ok
         }
         
         destination.setExpiryQueue(dest);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMXInvocation(t, this + " setExpiryQueue");
      }
   }
   
   public ObjectName getExpiryQueue()
   {
      return expiryQueueObjectName;
   }
   
   public long getRedeliveryDelay()
   {
      return destination.getRedeliveryDelay();
   }
   
   public void setRedeliveryDelay(long delay)
   {
      destination.setRedeliveryDelay(delay);
   }
   
   public int getMaxSize()
   {
      return destination.getMaxSize();
   }
   
   public void setMaxSize(int maxSize) throws Exception
   {
      destination.setMaxSize(maxSize);
   }
   
   public Element getSecurityConfig()
   {
      return destination.getSecurityConfig();
   }
   
   public void setSecurityConfig(Element securityConfig) throws Exception
   {
      try
      {
         if (started)
         {
            // push security update to the server
            serverPeer.getSecurityManager().setSecurityConfig(isQueue(), destination.getName(), securityConfig);  
         }
   
         destination.setSecurityConfig(securityConfig);
      }
      catch (Throwable t)
      {
         ExceptionUtil.handleJMXInvocation(t, this + " setSecurityConfig");
      }
   }

   /**
    * Get in-memory message limit
    * @return message limit
    */
   public int getFullSize()
   {
      return destination.getFullSize();
   }

   /**
    * Set in-memory message limit when destination is stopped.
    * @param fullSize the message limit
    */
   public void setFullSize(int fullSize)
   {
      if (started)
      {
         log.warn("FullSize can only be changed when destination is stopped");
         return;
      }      
      destination.setFullSize(fullSize);
   }

   /**
    * Get paging size
    * @return paging size
    */
   public int getPageSize()
   {
      return destination.getPageSize();
   }

   /**
    * Set paging size when destination is stopped.
    * @param pageSize the paging size
    */
   public void setPageSize(int pageSize)
   {
      if (started)
      {
         log.warn("PageSize can only be changed when destination is stopped");
         return;
      }
      destination.setPageSize(pageSize);
   }

   /**
    * Get write-cache size
    * @return cache size
    */
   public int getDownCacheSize()
   {
      return destination.getDownCacheSize();
   }

   /**
    * Set write-cache size when destination is stopped.
    * @param downCacheSize the cache size
    */
   public void setDownCacheSize(int downCacheSize)
   {
      if (started)
      {
         log.warn("DownCacheSize can only be changed when destination is stopped");
         return;
      }
      destination.setDownCacheSize(downCacheSize);
   }
   
   public boolean isClustered()
   {
      return destination.isClustered();
   }
   
   public void setClustered(boolean clustered)
   {
      if (started)
      {
         log.warn("Clustered can only be changed when destination is stopped");
         return;
      }
      destination.setClustered(clustered);
   }
   
   public boolean isCreatedProgrammatically()
   {
      return createdProgrammatically;
   }
   
   public int getMessageCounterHistoryDayLimit()
   {
      return destination.getMessageCounterHistoryDayLimit();
   }
   
   public void setMessageCounterHistoryDayLimit(int limit) throws Exception
   {
      destination.setMessageCounterHistoryDayLimit(limit);
   }
   
   public int getMaxDeliveryAttempts()
   {
      return destination.getMaxDeliveryAttempts();
   }
   
   public void setMaxDeliveryAttempts(int maxDeliveryAttempts)
   {
      destination.setMaxDeliveryAttempts(maxDeliveryAttempts);
   }
   
   // JMX managed operations ----------------------------------------
   
   public abstract void removeAllMessages() throws Exception;
   

   // Public --------------------------------------------------------

   public String toString()
   {
      String nameFromJNDI = destination.getJndiName();
      int idx = -1;
      if (nameFromJNDI != null)
      {
         idx = nameFromJNDI.lastIndexOf('/');
      }
      if (idx != -1)
      {
         nameFromJNDI = nameFromJNDI.substring(idx + 1);
      }
      StringBuffer sb = new StringBuffer();
      if (isQueue())
      {
         sb.append("Queue");
      }
      else
      {
         sb.append("Topic");
      }
      sb.append('[');
      if (destination == null)
      {
         sb.append("(NULL Destination)");
      }
      else if (destination.getName() == null)
      {
         sb.append("(destination.getName() == NULL)");
      }
      else
      {
         if (destination.getName().equals(nameFromJNDI))
         {
            sb.append(destination.getJndiName());
         }
         else
         {
            sb.append(destination.getJndiName()).append(", name=").append(destination.getName());
         }
      }
      sb.append(']');
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   /**
    * List message counters as HTML table
    * 
    * @return String
    */
   protected String listMessageCounterAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;
      
      String ret = "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"  +
                   "<tr>"                  +
                   "<th>Type</th>"         +
                   "<th>Name</th>"         +
                   "<th>Subscription</th>" +
                   "<th>Durable</th>"      +
                   "<th>Count</th>"        +
                   "<th>CountDelta</th>"   +
                   "<th>Depth</th>"        +
                   "<th>DepthDelta</th>"   +
                   "<th>Last Add</th>"     +
                   "</tr>";
      
      for( int i=0; i<counters.length; i++ )
      {
         String            data = counters[i].getCounterAsString();
         StringTokenizer   token = new StringTokenizer( data, ",");
         String            value;
         
         ret += "<tr bgcolor=\"#" + ( (i%2)==0 ? "FFFFFF" : "F0F0F0") + "\">";

         ret += "<td>" + token.nextToken() + "</td>"; // type
         ret += "<td>" + token.nextToken() + "</td>"; // name
         ret += "<td>" + token.nextToken() + "</td>"; // subscription
         ret += "<td>" + token.nextToken() + "</td>"; // durable

         ret += "<td>" + token.nextToken() + "</td>"; // count
         
         value = token.nextToken(); // countDelta

         if( value.equalsIgnoreCase("0") )
             value = "-";
             
         ret += "<td>" + value + "</td>";
         
         ret += "<td>" + token.nextToken() + "</td>"; // depth
         
         value = token.nextToken(); // depthDelta
         
         if( value.equalsIgnoreCase("0") )
             value = "-";
             
         ret += "<td>" + value + "</td>";

         ret += "<td>" + token.nextToken() + "</td>"; // date last add

         ret += "</tr>";
      }
      
      ret += "</table>";
      
      return ret;
   }      
   
   /**
    * List destination message counter history as HTML table
    * 
    * @return String
    */
   protected String listMessageCounterHistoryAsHTML(MessageCounter[] counters)
   {
      if (counters == null)
         return null;
      
      String           ret = "";
               
      for( int i=0; i<counters.length; i++ )
      {
         // destination name
         ret += ( counters[i].getDestinationTopic() ? "Topic '" : "Queue '" );
         ret += counters[i].getDestinationName() + "'";
         
         if( counters[i].getDestinationSubscription() != null )
            ret += "Subscription '" + counters[i].getDestinationSubscription() + "'";
            
                     
         // table header
         ret += "<table width=\"100%\" border=\"1\" cellpadding=\"1\" cellspacing=\"1\">"  +
                "<tr>"                  +
                "<th>Date</th>";

         for( int j = 0; j < 24; j++ )
            ret += "<th width=\"4%\">" + j + "</th>";

         ret += "<th>Total</th></tr>";

         // get history data as CSV string         
         StringTokenizer tokens = new StringTokenizer( counters[i].getHistoryAsString(), ",\n");
         
         // get history day count
         int days = Integer.parseInt( tokens.nextToken() );
         
         for( int j=0; j<days; j++ )
         {
            // next day counter row 
            ret += "<tr bgcolor=\"#" + ((j%2)==0 ? "FFFFFF" : "F0F0F0") + "\">";
         
            // date 
            ret += "<td>" + tokens.nextToken() + "</td>";
             
            // 24 hour counters
            int total = 0;
            
            for( int k=0; k<24; k++ )
            {
               int value = Integer.parseInt( tokens.nextToken().trim() );
            
               if( value == -1 )
               {
                    ret += "<td></td>";
               }  
               else
               {
                    ret += "<td>" + value + "</td>";
                    
                    total += value;
               } 
            }

            ret += "<td>" + total + "</td></tr>";
         }

         ret += "</table><br><br>";
      }

      return ret;
   }

   protected abstract boolean isQueue();

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
