/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tx;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class ResourceManager
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private ConcurrentReaderHashMap transactions = new ConcurrentReaderHashMap();
   
   private long idSequence;

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ResourceManager.class);
   
   private static ResourceManager instance = new ResourceManager();
   
   public static ResourceManager instance()
   {
      return instance;
   }
   
   // Constructors --------------------------------------------------

   private ResourceManager()
   {
   }
   
   // Public --------------------------------------------------------
   
   public Object createLocalTx()
   {
      LocalTx tx = new LocalTx(getNextTxId());
      transactions.put(tx.getId(), tx);
      return tx.getId();
   }

   private LocalTx getTx(Object Xid)
   {
      LocalTx tx = (LocalTx) transactions.get(Xid);
      if (tx == null)
         throw new IllegalArgumentException("No such Xid:" + Xid);
      return tx;
   }

   public void addMessage(Object Xid, Message m)
   {
      LocalTx tx = getTx(Xid);
      tx.addMessage(m);
   }

   // TODO
   public void addAck(Object Xid, Object ack)
   {
      LocalTx tx = getTx(Xid);
      tx.addAck(ack);
   }

   public Object commit(Object Xid, SessionDelegate sd) throws JMSException
   {
      log.debug("Committing tx: " + Xid);
      LocalTx tx = getTx(Xid);
      if (tx.getMessages().size() > 0 || tx.getAcks().size() > 0)
      {
         log.debug("There is stuff to send");
         sd.sendTransaction(tx);
      }
      transactions.remove(Xid);
      return createLocalTx();
   }

   public Object rollback(Object Xid)
   {
      log.debug("Rolling back tx: " + Xid);
      LocalTx tx = getTx(Xid);
      transactions.remove(Xid);
      return createLocalTx();
   }
   
   
   //TODO XA Stuff

   // Interceptor implementation ------------------------------------
   
   // Protected ------------------------------------------------------
   
   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private synchronized Long getNextTxId()
   {
      return new Long(idSequence++);
   }
   
   // Inner Classes --------------------------------------------------
   
   
   

}
