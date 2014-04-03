import java.util.Vector;

import sofia_kp.KPICore;
import sofia_kp.SSAP_XMLTools;
import sofia_kp.SSAP_sparql_response;
import sofia_kp.iKPIC_subscribeHandler;



/**
 *  To explain basic information about java KPI usage, no execution supported here please make your own first KP
 *  and use this class as a guide on how to use the KPI, or as a quick reference
 *  
 * 
 */




public class KPIPrimitiveUsage implements iKPIC_subscribeHandler{

	/**
	 * Typical declarations
	 */

	public KPICore kp;  //direct interface with the SIB
	public SSAP_XMLTools xml_tools;  // utility methods to compose messages and manage responses
	public String xml =""; //conventionally used for storing the messages from the SIB
	public boolean ack = false; // Conventionally used for checking SIB response

	/**
	 * SIB constants to be opportunely modified statically or at run-time in order to interact with the SIB
	 */
	public String SIB_Host = "192.168.0.103";
	public  int SIB_Port = 10010;
	public  String SIB_Name = "X"; //at Jan 2013 this is still not a relevant parameter checked by SIB, but must be specified 

	//public Handler handler = new Handler();  //This if we want to specify an handler for subscriptionsdifferent from this class

	public static void main(String [] argv)
	{
		KPIPrimitiveUsage test = new KPIPrimitiveUsage();
		test.test_primitives();
	}

	public void test_primitives()
	{

		KPICore kp = new KPICore(SIB_Host, SIB_Port, SIB_Name);
		xml_tools = new SSAP_XMLTools();
		//Remove debug and error print
		kp.disable_debug_message();
		kp.disable_error_message();

		Vector<Vector<String>> triples = new Vector<Vector<String>>();
		Vector<Vector<String>> triples_ins = new Vector<Vector<String>>();  //Structure that can be useful in many programs
		Vector<Vector<String>> triples_rem = new Vector<Vector<String>>();  //Structure that can be useful in many programs

		Vector<String> triple = new Vector<String>();//Structure that can be useful in many programs

		//creating 5 example triples
		String ns = "http://examplens#";
		for(int i = 0; i < 5; i++)
		{
			triple = new Vector<String>();
			triple.add(ns+ "subject_" + i);
			triple.add(ns+ "predicate_" + i);
			triple.add(ns+ "object_" + i);
			triple.add("uri");
			triple.add("uri");
			triples_ins.add(triple);
			triples_rem.add(triple);
		}


		//Join


		xml = kp.join();
		ack = xml_tools.isJoinConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error Joining the SIB");
		}

		//  insert triples

		xml = kp.insert(triples_ins);  //to be launched when triple_ins contains triples

		ack = xml_tools.isInsertConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error Inserting into the SIB");
		}

		xml = kp.insert(ns + "subject_triple_old", ns + "predicate_triple_old",ns + "object_triple_old","uri","uri"); //we are inserting it to delete or update it later
		xml = kp.insert(ns + "subject_triple_old", ns + "predicate_triple_old",ns + "object_triple_old2","uri","uri"); //we are inserting it to delete or update it later
		ack = xml_tools.isInsertConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error Inserting into the SIB");
		}
		//  remove triples: from coding point of view is the same as insert, but it is possible to use wildcards, for example

		xml = kp.remove(ns + "subject_triple_old", ns + "predicate_triple_old",xml_tools.ANYURI,"uri","uri"); //two triples are deleted
		ack = xml_tools.isRemoveConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error removing from the SIB");
		}

		xml = kp.remove(triples_rem); 
		ack = xml_tools.isRemoveConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error removing from the SIB");
		}

		kp.insert(ns + "subject_triple_old", ns + "predicate_triple_old",ns + "object_triple_old","uri","uri"); //we are inserting it to delete or update it later

		/*
		 *  updating triples as before we can update with set of tripples using triples_ins and triples_rem or single triples specifying the triples element
		 *  int the function we must specify the new content first and the old content( to be deleted) later. In the SIB the old content is deleted first and the is added 
		 *  the new content. The update is not used only to update sing arcs, but also as a shortcut for two independent and successive remove and insert
		 *  operazions( in this order )
		 */

		xml = kp.update(ns + "subject_triple_new", ns + "predicate_triple_new",ns + "object_triple_new","uri","uri" , ns + "subject_triple_old", ns + "predicate_triple_old",ns + "object_triple_old","uri","uri");
		ack = xml_tools.isUpdateConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error updating the SIB");
		}

		/*
		 * Subription:
		 * If this Class doesn't @code {implements iKPIC_subscribeHandler} and if it needs to subscribe to the SIB, 
		 * we must specify a subscribe handler implementing the needed interface . 
		 * 
		 * two kinds of subscription: RDF-M3 and SPARQL
		 * 
		 * The subscription RDF-M3 is based on triple patterns, it accepts one or more triple patterns and returns all the existing triples matching the patterns
		 * then a notification is received when triples matching one or more of the patterns are inserted or removed
		 * 
		 * The SPARQL subscription is based on SPARQL queries, the subscription returns all the results matching the query (like a query to SIB)
		 * then a  notification is received when the results of the query change. Only the new or old results are sent back in notifications
		 * 
		 * 
		 */


		//	kp.setEventHandler(handler)This if we want to specify an handler for subscriptionsdifferent from this class

		kp.setEventHandler(this);// The handler of this object is this object itself


		xml = kp.subscribeRDF(ns+ "subject_1", null , null , "uri");//null is an alias equivalent to: xmlTools.ANYURI
		String subID_1 = null;		
		if(xml_tools.isSubscriptionConfirmed(xml))
		{
			try
			{
				subID_1=xml_tools.getSubscriptionID(xml);
			}
			catch(Exception e)
			{

			}
		}
		else
		{
			System.out.println ("Error during subscription");
		}

		kp.insert(triples_ins); //This should fire a notification

		xml = kp.subscribeSPARQL("Select ?a ?b ?c where { ?a ?b ?c }");// SPARQL subscription to all triples
		String subID_2=null;		
		if(this.xml_tools.isSubscriptionConfirmed(xml))
		{
			try
			{
				subID_2=xml_tools.getSubscriptionID(xml);
			}
			catch(Exception e)
			{

			}
		}
		else
		{
			System.out.println ("Error during subscription");
		}
		SSAP_sparql_response resp = xml_tools.get_SPARQL_query_results(xml);//An object to manage the sparql response

		System.out.println(resp.print_as_string());//the representation of variables and corresponding values in human readable format


		/*
		 * Unsubscription
		 */

		
		if(subID_1!= null)
		{
			xml = kp.unsubscribe(subID_1);
		}



		/* 
		 * Query RDF-M3:
		 * The query RDF-M3 is based on triple patterns, it accepts one or more triple patterns and returns all the existing triples matching the patterns
		 * 
		 *  Query SPARQL:
		 * The  SPARQL query primitive is based on a SPARQL query, it accepts one SPARQL query the result can be wrapped in an object.
		 * 
		 */


		xml=kp.queryRDF (ns+ "subject_1", xml_tools.ANYURI, null, "uri","uri");//null is an alias equivalent to: xmlTools.ANYURI

		ack = xml_tools.isQueryConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error during RDF-M3 query");
		}    
		else
		{
			triples = xml_tools.getQueryTriple(xml);
		}

		xml=kp.querySPARQL("Select ?a ?b ?c where { ?a ?b ?c }") ;
		ack = xml_tools.isQueryConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error during SPARQL query");
		}    
		else
		{
			SSAP_sparql_response query_response = xml_tools.get_SPARQL_query_results(xml);
			System.out.println(query_response.print_as_string());
		}

		/*
		 *LEAVE
		 */

		xml = kp.leave();
		ack = xml_tools.isLeaveConfirmed(xml);
		if(!ack)
		{
			System.out.println ("Error during LEAVE");
		}   
	}





	/**
	 * Example of subscription handler, in the code is better to start a new thread because if many notifications arrives with high
	 * frequency on a single Thread, then the is the possibility of a Parsing Exception
	 */

	@Override
	public void kpic_SIBEventHandler( String xml_received)
	{
		final String xml = xml_received;


		new Thread(
				new Runnable() {
					public void run() {

						
						String id = "";

						SSAP_XMLTools xmlTools = new SSAP_XMLTools();
						boolean isunsubscription = xmlTools.isUnSubscriptionConfirmed(xml);
						if(!isunsubscription)
						{
							String k = xmlTools.getSSAPmsgIndicationSequence(xml);
							id = xmlTools.getSubscriptionID(xml);
							

							if(xmlTools.isRDFNotification(xml))
							{
								Vector<Vector<String>> triples_n = new Vector<Vector<String>>();
								triples_n = xmlTools.getNewResultEventTriple(xml);
								Vector<Vector<String>> triples_o = new Vector<Vector<String>>();
								triples_o = xmlTools.getObsoleteResultEventTriple(xml);
								String temp = "Notif. " + k + " id = " + id +"\n";
								for(int i = 0; i < triples_n.size(); i++ )
								{
									temp+="New triple s =" + triples_n.elementAt(i).elementAt(0) + "  + predicate" + triples_n.elementAt(i).elementAt(1) + "object =" + triples_n.elementAt(i).elementAt(2) +"\n";
								}
								for(int i = 0; i < triples_o.size(); i++ )
								{
									temp+="Obsolete triple s =" + triples_o.elementAt(i).elementAt(0) + "  + predicate" + triples_o.elementAt(i).elementAt(1) + "object =" + triples_o.elementAt(i).elementAt(2) + "\n";
								}
								System.out.println(temp);
							}
							else
							{
								System.out.println("Notif. " + k + " id = " + id +"\n");
								SSAP_sparql_response resp_new = xmlTools.get_SPARQL_indication_new_results(xml);
								SSAP_sparql_response resp_old = xmlTools.get_SPARQL_indication_obsolete_results(xml);
								if (resp_new != null)
								{
									System.out.println("new: \n " + resp_new.print_as_string());
								}
								if (resp_old != null)
								{
									System.out.println("obsolete: \n " + resp_old.print_as_string());
								}
							}
						}

					}
				}).start();

	}

}
