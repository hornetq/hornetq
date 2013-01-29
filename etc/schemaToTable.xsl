<?xml version="1.0" encoding="ISO-8859-1"?>

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema">

<!--
   XSLT for generating manual reference from XML Schema.

   TODO:
   - handling of (string) simple types with restriction (join values on '|')
   - handling of default values better expressed as multiple integers
   e.g. '1014 * 1024'
   - generating Java code.
-->

<xsl:output method="xml"/>

<xsl:template match="/">
    <xsl:for-each select="xsd:schema/xsd:element/xsd:complexType/xsd:all/xsd:element">
      <row><xsl:text>&#xa;</xsl:text>
        <entry><xsl:text>&#xa;</xsl:text>
          <xsl:element name="link">
            <xsl:attribute name="linkend">
              <xsl:value-of select="xsd:annotation/@linkend"/>
            </xsl:attribute>
            <xsl:value-of select="@name"/><xsl:text>&#xa;</xsl:text>
          </xsl:element><xsl:text>&#xa;</xsl:text>
        </entry><xsl:text>&#xa;</xsl:text>
        <entry>
          <xsl:value-of select="@type"/>
        </entry><xsl:text>&#xa;</xsl:text>
        <entry>
          <xsl:value-of select="xsd:annotation/xsd:documentation"/>
        </entry><xsl:text>&#xa;</xsl:text>
        <entry>
          <xsl:value-of select="@default"/>
        </entry><xsl:text>&#xa;</xsl:text>
      </row><xsl:text>&#xa;</xsl:text><xsl:text>&#xa;</xsl:text>
    </xsl:for-each>
</xsl:template>

</xsl:stylesheet>
