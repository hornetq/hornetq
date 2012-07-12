package org.hornetq.rest.queue.push.xml;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class Authentication implements Serializable
{
   private static final long serialVersionUID = -6218446923598032634L;
   private AuthenticationType type;

   @XmlElementRef
   public AuthenticationType getType()
   {
      return type;
   }

   public void setType(AuthenticationType type)
   {
      this.type = type;
   }

   @Override
   public String toString()
   {
      return "Authentication{" +
              "type=" + type +
              '}';
   }
}
