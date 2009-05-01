/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.ra;

import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;

import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.SecurityException;
import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;

import org.jboss.messaging.core.logging.Logger;

/**
 * Credential information
 * 
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: 71554 $
 */
public class JBMCredential implements Serializable
{
   /** Serial version UID */
   static final long serialVersionUID = 210476602237497193L;

   /** The logger */
   private static final Logger log = Logger.getLogger(JBMCredential.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The user name */
   private String userName;

   /** The password */
   private String password;

   /**
    * Private constructor
    */
   private JBMCredential()
   {
      if (trace)
      {
         log.trace("constructor()");
      }
   }

   /**
    * Get the user name
    * @return The value
    */
   public String getUserName()
   {
      if (trace)
      {
         log.trace("getUserName()");
      }

      return userName;
   }

   /**
    * Set the user name
    * @param userName The value
    */
   private void setUserName(final String userName)
   {
      if (trace)
      {
         log.trace("setUserName(" + userName + ")");
      }

      this.userName = userName;
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (trace)
      {
         log.trace("getPassword()");
      }

      return password;
   }

   /**
    * Set the password
    * @param password The value
    */
   private void setPassword(final String password)
   {
      if (trace)
      {
         log.trace("setPassword(****)");
      }

      this.password = password;
   }

   /**
    * Get credentials
    * @param mcf The managed connection factory
    * @param subject The subject
    * @param info The connection request info
    * @return The credentials
    * @exception SecurityException Thrown if the credentials cant be retrieved
    */
   public static JBMCredential getCredential(final ManagedConnectionFactory mcf,
                                             final Subject subject,
                                             final ConnectionRequestInfo info) throws SecurityException
   {
      if (trace)
      {
         log.trace("getCredential(" + mcf + ", " + subject + ", " + info + ")");
      }

      JBMCredential jc = new JBMCredential();
      if (subject == null && info != null)
      {
         jc.setUserName(((JBMConnectionRequestInfo)info).getUserName());
         jc.setPassword(((JBMConnectionRequestInfo)info).getPassword());
      }
      else if (subject != null)
      {
         PasswordCredential pwdc = GetCredentialAction.getCredential(subject, mcf);

         if (pwdc == null)
         {
            throw new SecurityException("No password credentials found");
         }

         jc.setUserName(pwdc.getUserName());
         jc.setPassword(new String(pwdc.getPassword()));
      }
      else
      {
         throw new SecurityException("No Subject or ConnectionRequestInfo set, could not get credentials");
      }

      return jc;
   }

   /**
    * String representation
    * @return The representation
    */
   @Override
   public String toString()
   {
      if (trace)
      {
         log.trace("toString()");
      }

      return super.toString() + "{ username=" + userName + ", password=**** }";
   }

   /**
    * Privileged class to get credentials
    */
   private static class GetCredentialAction implements PrivilegedAction<PasswordCredential>
   {
      /** The subject */
      private final Subject subject;

      /** The managed connection factory */
      private final ManagedConnectionFactory mcf;

      /**
       * Constructor
       * @param subject The subject
       * @param mcf The managed connection factory
       */
      GetCredentialAction(final Subject subject, final ManagedConnectionFactory mcf)
      {
         if (trace)
         {
            log.trace("constructor(" + subject + ", " + mcf + ")");
         }

         this.subject = subject;
         this.mcf = mcf;
      }

      /**
       * Run
       * @return The credential
       */
      public PasswordCredential run()
      {
         if (trace)
         {
            log.trace("run()");
         }

         Set<PasswordCredential> creds = subject.getPrivateCredentials(PasswordCredential.class);
         PasswordCredential pwdc = null;

         for (PasswordCredential curCred : creds)
         {
            if (curCred.getManagedConnectionFactory().equals(mcf))
            {
               pwdc = curCred;
               break;
            }
         }
         return pwdc;
      }

      /**
       * Get credentials
       * @param subject The subject
       * @param mcf The managed connection factory
       * @return The credential
       */
      static PasswordCredential getCredential(final Subject subject, final ManagedConnectionFactory mcf)
      {
         if (trace)
         {
            log.trace("getCredential(" + subject + ", " + mcf + ")");
         }

         GetCredentialAction action = new GetCredentialAction(subject, mcf);
         return AccessController.doPrivileged(action);
      }
   }
}
