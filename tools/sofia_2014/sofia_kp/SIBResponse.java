package sofia_kp;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.input.SAXBuilder;



public class SIBResponse {


	public  String Message= "";
	public  String MessageType = "";
	public  String TransactionType = "";
	public  String Status = "";
	public  String transactionID;
	public  String node_id="";
	public  String space_id="";
	public  String triples_encoding="";
	public  String subscription_id="";
	public  String update_id="";
	public  String indication_sequence;
	public Vector<Vector<String>> new_results= new Vector<Vector<String> >();
	public Vector<Vector<String>> obsolete_results= new Vector<Vector<String> >();
	public Vector<Vector<String>> query_results= new Vector<Vector<String> >();
	public SSAP_sparql_response sparqlquery_results= null;
	public SSAP_sparql_response sparql_ind_new_results= null;
	public SSAP_sparql_response sparql_ind_old_results= null;
	public String rdf_xml_graph ="";
	public String rdf_xml_remove_graph ="";
	public String queryType = "";

	public SIBResponse() {
		// TODO Auto-generated constructor stub


	}

	public SIBResponse(String xml)
	{
		this.Message = xml;
		Document sparql_response_document;

		sparql_response_document = new Document();
		try {
			sparql_response_document = loadXMLFromString(xml);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sparql_response_document = null;
			this.Status = "Error While parsing message ";
		}

		//	        GET ROOT ELEMENT + NAMESPACE
		if(sparql_response_document != null)
		{
			Element root = sparql_response_document.getRootElement();
			Namespace ns;

			//			GET ROOT CHILDREN ()
			this.MessageType = root.getChildText("message_type");
			this.transactionID = root.getChildText("transaction_id");
			this.TransactionType = root.getChildText("transaction_id");
			this.node_id = root.getChildText("node_id");
			this.space_id = root.getChildText("space_id");


			List <Element> parameterList = root.getChildren("parameter");
			Iterator <Element> parameterIter = parameterList.iterator();

			ArrayList<Element> parToProcess = new ArrayList<Element>();

			while(parameterIter.hasNext())
			{
				Element Parameter = parameterIter.next();
				String parName = Parameter.getAttributeValue("name");

				switch (parName)
				{

				case "status": this.Status = Parameter.getText(); 
				break;
				case "subscription_id": this.subscription_id = Parameter.getText();
				break;
				case "update_id" : this.update_id = Parameter.getText();
				break;
				case "type": this.queryType = Parameter.getText(); //here the query type if needed 
				break;
				//case "query": {} //here the  RDF query to send, not needed for answers

				case "new_results": parToProcess.add(Parameter);
				break;
				case "obsolete_results": parToProcess.add(Parameter);
				break;
				case "results": parToProcess.add(Parameter);
				break;
				default : System.out.println("Error while reading parameter name");
				break;
				}
			}

			for(int i = 0; i < parToProcess.size(); i++)
			{
				System.out.println ("HIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII" + parToProcess.get(i).toString());
				Element par = parToProcess.get(i);
				String parName = par.getAttributeValue("name");
				System.out.println("Par name to process: " + parName);
				if(par.getChild("triple_list")!= null)
				{

					this.queryType = "RDF-M3";
					Vector<Vector<String>> triplev = new Vector<Vector<String>>();
					Element el = par.getChild ("triple_list");

					List<Element> triples =  el.getChildren("triple");
					Iterator<Element> it = triples.iterator();

					while(it.hasNext())
					{   Vector<String> singleton=new Vector<String>();

					Element etriple = it.next();

					singleton.add(etriple.getChild("subject").getText());

					singleton.add(etriple.getChild("predicate").getText());
					singleton.add(etriple.getChild("object").getText());
					singleton.add(etriple.getChild("object").getAttributeValue("type"));

					triplev.add(singleton);			
					}//while(it.hasNext())
					if (parName.equals("results"))
					{
						this.query_results = triplev;
					}
					else if (parName.equals("new_results"))
					{
						this.new_results = triplev;
					}
					else if (parName.equals("parToProcess"))
					{
						this.obsolete_results = triplev;
					}

				}
				else if(par.getChild("sparql", Namespace.getNamespace("http://www.w3.org/2005/sparql-results#"))!= null)
				{
					this.queryType = "sparql";
					SSAP_sparql_response sparql_resp = new SSAP_sparql_response(par.getChild("sparql", Namespace.getNamespace("http://www.w3.org/2005/sparql-results#")));
					if (parName.equalsIgnoreCase("results"))
					{
						this.sparqlquery_results = sparql_resp;
					}
					else if (parName.equalsIgnoreCase("new_results"))
					{
						this.sparqlquery_results = sparql_resp;
					}
					else if (parName.equalsIgnoreCase("parToProcess"))
					{
						this.sparqlquery_results = sparql_resp;
					}
				}

			}
		}
	}



	private static Document loadXMLFromString(String xml) throws Exception
	{
		SAXBuilder builder = new SAXBuilder();
		Document doc;
		//System.out.println("xml = " + xml);
		try
		{
			doc = builder.build(new ByteArrayInputStream(xml.getBytes()));
		}
		catch (Throwable e)
		{
			doc = null;
			e.printStackTrace();
		}
		return doc;
	}

	@Override
	public String toString()
	{
		return this.Message;
	}



}
