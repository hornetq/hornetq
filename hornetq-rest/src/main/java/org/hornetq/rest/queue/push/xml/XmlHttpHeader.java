package org.hornetq.rest.queue.push.xml;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
@XmlRootElement(name = "header")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class XmlHttpHeader implements Serializable
{
   private static final long serialVersionUID = -390039194544718601L;
   private String name;
   private String value;

   @XmlAttribute
   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   @XmlValue
   public String getValue()
   {
      return value;
   }

   public void setValue(String value)
   {
      this.value = value;
   }

   @Override
   public String toString()
   {
      return "XmlHttpHeader{" +
              "name='" + name + '\'' +
              ", value='" + value + '\'' +
              '}';
   }
}
