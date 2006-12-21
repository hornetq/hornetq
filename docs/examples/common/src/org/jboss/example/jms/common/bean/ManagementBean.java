/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.common.bean;

import java.rmi.RemoteException;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1766 $</tt>

 * $Id: StatelessSessionExampleBean.java 1766 2006-12-11 22:29:27Z ovidiu.feodorov@jboss.com $
 */
public class ManagementBean implements SessionBean
{
   public void killAS() throws Exception
   {
      System.out.println("######");
      System.out.println("######");
      System.out.println("######");
      System.out.println("######");
      System.out.println("###### SIMULATING A FAILURE, KILLING THE VM!");
      System.out.println("######");
      System.out.println("######");
      System.out.println("######");
      System.out.println("######");

      Runtime.getRuntime().halt(1);
   }

   public void setSessionContext(SessionContext ctx) throws EJBException, RemoteException
   {
   }

   public void ejbCreate()
   {
   }

   public void ejbRemove() throws EJBException
   {
   }

   public void ejbActivate() throws EJBException, RemoteException
   {
   }

   public void ejbPassivate() throws EJBException, RemoteException
   {
   }

}
