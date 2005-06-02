/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tx;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TransactionRolledBackException;

import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * 
 * 
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com>Tim Fox </a>
 */
public class ResourceManager
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
	//TODO - If there are a lot of messages/acks in a tx
	//we need to sensibly deal with this to avoid exhausting memory - 
	//perhaps spill over into a persistent store
   private ConcurrentHashMap transactions = new ConcurrentHashMap();
   
   private long idSequence;
	
	private ConnectionDelegate connection;

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ResourceManager.class);
   
           
   // Constructors --------------------------------------------------

   public ResourceManager(ConnectionDelegate connection)
   {
		this.connection = connection;
   }
   
   // Public --------------------------------------------------------
   
   public Object createLocalTx()
   {
      TxInfo tx = new TxInfo(getNextTxId());
      transactions.put(tx.getId(), tx);
      return tx.getId();
   }

   private TxInfo getTx(Object Xid)
   {
		TxInfo tx = (TxInfo) transactions.get(Xid);
      if (tx == null)
         throw new IllegalArgumentException("No such Xid:" + Xid);
      return tx;
   }

   public void addMessage(Object Xid, Message m)
   {
		TxInfo tx = getTx(Xid);
      tx.addMessage(m);
   }

   // TODO
   public void addAck(Object Xid, AckInfo ackInfo)
   {
		TxInfo tx = getTx(Xid);
      tx.addAck(ackInfo);
   }

   public Object commit(Object Xid) throws JMSException
   {
      log.debug("Committing tx: " + Xid);
		try
		{
			TxInfo tx = getTx(Xid);
	      if (tx.getMessages().size() > 0 || tx.getAcks().size() > 0)
	      {
	         log.debug("There is stuff to send");
	         connection.sendTransaction(tx);
	      }
		}
		catch (Throwable t)
		{
			log.error("Failed to commit tx", t);
			throw new TransactionRolledBackException("Failed to commit tx: " + t.getMessage());
		}
		finally
		{
	      transactions.remove(Xid);
		}
      return createLocalTx();
   }

   public Object rollback(Object Xid)
   {
      log.debug("Rolling back tx: " + Xid);
		TxInfo tx = getTx(Xid);
      transactions.remove(Xid);
      return createLocalTx();
   }
   
   
   // TODO XA Stuff

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
