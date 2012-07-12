package org.hornetq.rest.queue.push.xml;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlRootElement(name = "basic-auth")
@XmlType(propOrder = {"username", "password"})
public class BasicAuth extends AuthenticationType
{
   private static final long serialVersionUID = 2052716241089832934L;
   private String username;
   private String password;

   public String getUsername()
   {
      return username;
   }

   public void setUsername(String username)
   {
      this.username = username;
   }

   public String getPassword()
   {
      return password;
   }

   public void setPassword(String password)
   {
      this.password = password;
   }

   @Override
   public String toString()
   {
      return "BasicAuth{" +
              "username='" + username + '\'' +
              ", password='" + password + '\'' +
              '}';
   }
}
