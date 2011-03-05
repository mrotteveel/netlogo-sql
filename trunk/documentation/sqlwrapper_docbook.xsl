<?xml version="1.0"?>
<xsl:stylesheet 
     xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
     xmlns:fo="http://www.w3.org/1999/XSL/Format"
     xmlns:d="http://docbook.org/ns/docbook"
     exclude-result-prefixes="d"
     version="1.0">

	<xsl:import href="docbook.xsl"/>

	<xsl:attribute-set name="monospace.verbatim.properties" use-attribute-sets="verbatim.properties monospace.properties">
	  <xsl:attribute name="text-align">start</xsl:attribute>
	  <xsl:attribute name="wrap-option">wrap</xsl:attribute>
	</xsl:attribute-set>

</xsl:stylesheet>