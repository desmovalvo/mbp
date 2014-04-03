package src;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

import sofia_kp.KPICore;
import sofia_kp.SSAP_XMLTools;



/**
 * Class to be used in order to insert an ontology, stored in a RDF/XML file, into the spacified SIB
 * Jena libraries, Jdom and Java KPI must be properly included into classpath
 * 
 *
 */

public class OntologyLoader {
	
	String ip = "127.0.0.1";
	int port = 10010;
	String ontologyFile= "ontologies\\example.owl";
	KPICore kp = null;
	SSAP_XMLTools ssap = null;
	String xml = null;
	boolean ack = false;
	
	public static void main(String[] argv)
	{
		OntologyLoader test = new OntologyLoader();
	OntModel model = ModelFactory.createOntologyModel();
	InputStream in = FileManager.get().open( test.ontologyFile );
	if (in == null) {
	    throw new IllegalArgumentException(
	                                 "File: " + test.ontologyFile + " not found");
	}

	// read the RDF/XML file
	model.read(in, null);
	
	test.kp = new KPICore(test.ip, test.port, "X");
	test.kp.join();
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	model.write(baos);
	test.kp.insert_rdf_xml(baos.toString());
	}

}
