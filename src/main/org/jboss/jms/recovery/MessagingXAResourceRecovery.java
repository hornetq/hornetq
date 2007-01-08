package org.jboss.jms.recovery;

import java.sql.SQLException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * 
 * A MessagingXAResourceRecovery
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessagingXAResourceRecovery implements XAResourceRecovery
{   
   private boolean trace = log.isTraceEnabled();
   
   private static final Logger log = Logger.getLogger(MessagingXAResourceRecovery.class);
   
   private boolean working = false;
   
   private XAResource xaRes = null;
   
   private String xaConnFactory = null;
   
   public MessagingXAResourceRecovery()
   {
   }
   
   public XAResource getXAResource() throws SQLException
   {
      if (trace) { log.trace("returning the xaresource " + xaRes); }
      return xaRes;
   }
   
   /**
    * This method returns the Messaging XAResource reference.
    * You have to pass the jndi name of the XAConnectionFactory
    * via the JBossTS RecoveryManager's properties
    * @return
    */
   public XAResource initXAResource()
   {
      if(trace)
      {
         log.trace("Initialising xaresource..");
      }
      try
      {
         Context ctx = new InitialContext();
         
         XAConnectionFactory cf = (XAConnectionFactory) ctx
         .lookup(xaConnFactory);
         
         XAConnection xaConn = cf.createXAConnection();
         
         XASession session = xaConn.createXASession();
         xaRes = session.getXAResource();
         
         if(trace)
            log.trace("Found the xares: "+xaRes);
         
      }
      catch (Exception e)
      {
         // You may get this exception when the messaging server
         // is not fully booted up. Nothing to worry, it'll keep
         // trying until successful.
         log.warn("XAConnectionFactory is not found. \n" +
                  "The messaging server is not yet initialized.\n" +
         "we'll try again once server is fully back");
         
      }
      
      return xaRes;
   }
   
   /**
    * This method is used to pass any
    * intialisation parameters to this
    * class
    * @param param
    * @return
    * @throws SQLException
    */
   public boolean initialise(String param) throws SQLException
   {
      if(trace) { log.trace("Passed in parameter: " + param); }
      
      if (param != null)
      {
         // param is in the form of name=value
         String value = param.substring(param.indexOf("=") + 1);
         
         if(trace) { log.trace("The connection factory is " + value); }
         
         xaConnFactory = value;
      }
      else
      {
         log.debug("The XA connection factory parameter is null. " +
         "Using the default 'XAConnectionFactory'");
         
         xaConnFactory = "XAConnectionFactory";
      }
      return true;
   }
   
   /**
    * This method checks whether there's an xa resource available
    *
    * @return
    */
   public boolean hasMoreResources()
   {      
      if (working)
      {
         return false;
      }
      
      if (xaRes == null)
      {
         xaRes = initXAResource();
      }
      
      // test the resource
      try
      {
         
         xaRes.getTransactionTimeout();
         
         working = true;         
      }
      catch (Exception ignored)
      {
         
         // ignore this exception
      }
      
      return working;
   }
}
