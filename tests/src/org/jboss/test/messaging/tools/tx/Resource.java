/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;


import java.rmi.Remote;
import java.rmi.RemoteException;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;


/**
 * Interface that provides operations invoked by the transaction service on each resource.
 *
 * @author <a href="mailto:reverbel@ime.usp.br">Francisco Reverbel</a>
 * @version $Revision$ 
 */
interface Resource extends Remote
{
   
   Vote prepare()
         throws RemoteException,
                TransactionAlreadyPreparedException,
                HeuristicMixedException,
                HeuristicHazardException;

   void rollback()
         throws RemoteException,
                HeuristicCommitException,
                HeuristicMixedException,
                HeuristicHazardException;

   void commit()
         throws RemoteException,
                TransactionNotPreparedException,
                HeuristicRollbackException,
                HeuristicMixedException,
                HeuristicHazardException;
   
   void commitOnePhase()
         throws RemoteException,
                HeuristicHazardException;
   
   void forget()
         throws RemoteException;

}
