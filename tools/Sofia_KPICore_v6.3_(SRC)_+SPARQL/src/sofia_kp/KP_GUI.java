package sofia_kp;

import java.awt.*;
import java.awt.event.*;
import java.util.Vector;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JLabel;




public class KP_GUI  extends Panel implements ActionListener, iKPIC_subscribeHandler 
{
	/**
	 * 
	 */
	KPICore kp = null;
	String user_kp_id="";	
	/**
	 * 
	 */
	SSAP_XMLTools xmlTools=null;
	
	JPanel jpConnection=null;
	JPanel jpJoinLeave=null;
	JPanel jpTextFieldsOld=null;
	JPanel jpConsole=null;
	JPanel jpTextFieldsNew=null;
	JPanel jpUpdate=null;
	JPanel jpSparql=null;		//SPARQL SECTION
	JPanel jpSubscriptions;

	JPanel jpMain=null;
	JPanel jpMemo=null;
	
	
	//Connection
	JTextField tfIP,tfPort,tfSSN;
	//old
	JTextField tfSO,tfPO,tfOO,tfSOType,tfOOType;
	//new
	JTextField tfSN,tfPN,tfON,tfSNType,tfONType;
	//SPARQL
	JTextField tfSQ;
	//JtextField 
	
	
	JButton bGo,bJoin,bLeave,bRDFQuery,bInsert,bRemove,bSubscribe,bUnsubscribe,bUpdate;
	JButton bInsertProtection,bRemoveProtection;
	
	// SPARQL buttons
	JButton bSPARQLquery;
	JButton bSPARQLsubscription;
	//subscriptions
	JComboBox cbSUB;

	/*
	 *  Components for service discovery
	 */
	
	JTextField tfSerName;
	JTextField otherParams;
	JButton bSearchRService;
	JComboBox cbSOFIADNSIPList;
	String[] SOFIADNS_IP = new String[1];
	
	//Synchronization
	JTextField tfKPID;
	JButton bSetKPID;
	
	JTextArea taMemo;
	
	JComboBox cbIPList;
    /*String[] SIB_IP = {
            "127.0.0.1",
            "localhost",
            "mml.arces.unibo.it"
            };*/
	int SIB_IP_TOT=4;
	String[] SIB_IP = new String[SIB_IP_TOT];
	
	String my_last_subscription="";
	
	JLabel label_sub;
	
	public KP_GUI()
	{
		JLabel labelx;
		
		/*0_________________________________________________________*/
		//Connection
		tfIP   = new JTextField("");
		/*-_________________________________________________________*/
       
		SIB_IP[0]="127.0.0.1";
        SIB_IP[1]="localhost";
        SIB_IP[2]="mml.arces.unibo.it";
        SIB_IP[3]="192.168.1.115";
		/*-_________________________________________________________*/
                
		cbIPList = new JComboBox(SIB_IP);     cbIPList.setEditable(true);
		/*-_________________________________________________________*/
		
		tfPort = new JTextField("");
		tfSSN  = new JTextField("");
		
		tfIP.setColumns(10);
		tfPort.setColumns(5);
		tfSSN.setColumns(5);
		
		
		
		bGo    = new JButton("INIT"); bGo.addActionListener(this);
		
		jpConnection=new JPanel(false);
		jpConnection.setLayout(new GridLayout(4, 1));
		JPanel line1=new JPanel(false);
		JPanel line2=new JPanel(false);
		JPanel line3=new JPanel(false);
		JPanel line4=new JPanel(false);
		
		line1.add( new JLabel("IP") );   line1.add(cbIPList);
		line1.add( new JLabel("PORT") ); line1.add(tfPort);
		line1.add( new JLabel("SSN") );  line1.add(tfSSN);
		line1.add( bGo );

		jpConnection.add( line1 );

		/*
		 * Components for service discovery
		 */
		//SOFIADNS_IP[0]="http://mml.arces.unibo.it/sofia.dns";
		SOFIADNS_IP[0]="http://mml.arces.unibo.it/sofia.service.registry";
		tfSerName    = new JTextField("sib");
		bSearchRService = new JButton("Search for Service");       bSearchRService.addActionListener(this);
		cbSOFIADNSIPList = new JComboBox(SOFIADNS_IP); cbSOFIADNSIPList.setEditable(true);
		
		cbSOFIADNSIPList.setSize(100, 20);
		tfSerName.setColumns(10);
		
		line2.add( new JLabel("SOFIA.DNS") );    line2.add(cbSOFIADNSIPList);		
		//E' tutto da mettere nella casella di testo dei parametri line2.add( new JLabel("SERVICE NAME") ); line2.add(tfSerName);
		//line2.add( bSearchAndGo );
		
		jpConnection.add( line2 );

		otherParams  = new JTextField("");
		otherParams.setColumns(40);

		line3.add( new JLabel("Service profile properties:") ); line3.add( otherParams );
		jpConnection.add( line3 );
		
		bSearchRService.setSize(100, 50);
		line4.add( bSearchRService );
		jpConnection.add( line4 );
		/*1_________________________________________________________*/

		bJoin    = new JButton("Join");  bJoin.addActionListener(this);
		bLeave   = new JButton("Leave"); bLeave.addActionListener(this);
		
		//Synchronization
		tfKPID    = new JTextField("");		  tfKPID.setColumns(10);
		bSetKPID  = new JButton("Re-Join with this KP ID"); bSetKPID.addActionListener(this);
		bSetKPID.setForeground(Color.BLUE);
		
		
		jpJoinLeave=new JPanel(false);
		jpJoinLeave.add(bJoin);
		jpJoinLeave.add(bLeave);	
		
		labelx = new JLabel("KP ID"); labelx.setForeground(Color.BLUE);
		jpJoinLeave.add( labelx );
		jpJoinLeave.add( tfKPID   );
		jpJoinLeave.add( bSetKPID );
		
		/*2_________________________________________________________*/
		//old
		tfSO     = new JTextField("");
		tfPO     = new JTextField("");
		tfOO     = new JTextField("");
		tfSOType = new JTextField("");
		tfOOType = new JTextField("");
		
		tfSO.setColumns(10);
		tfPO.setColumns(10);
		tfOO.setColumns(10);
		tfSOType.setColumns(10);
		tfOOType.setColumns(10);

		jpTextFieldsOld=new JPanel(false);
		//jpTextFieldsOld.add( new JLabel("Subject") );
		labelx = new JLabel("Subject (S)"); labelx.setForeground(Color.RED);
		jpTextFieldsOld.add( labelx );
		                                                jpTextFieldsOld.add( tfSO );
		//jpTextFieldsOld.add( new JLabel("Predicate") );
		labelx = new JLabel("Predicate (S)"); labelx.setForeground(Color.RED);
		jpTextFieldsOld.add( labelx );
		                                                
		                                                jpTextFieldsOld.add( tfPO ); 
		jpTextFieldsOld.add( new JLabel("Object") );    jpTextFieldsOld.add( tfOO );
		jpTextFieldsOld.add( new JLabel("Sub Type") );  jpTextFieldsOld.add( tfSOType );
		jpTextFieldsOld.add( new JLabel("Obj Type") );  jpTextFieldsOld.add( tfOOType );
		
		/*3_________________________________________________________*/

		bRDFQuery    = new JButton("RDF Query");   bRDFQuery.addActionListener(this);
		bInsert      = new JButton("Insert");      bInsert.addActionListener(this);
		bRemove      = new JButton("Remove");      bRemove.addActionListener(this);
		bSubscribe   = new JButton("Subscribe");   bSubscribe.addActionListener(this);
		bUnsubscribe = new JButton("Unsubscribe"); bUnsubscribe.addActionListener(this);
		
		jpConsole=new JPanel(false);
		jpConsole.add(bRDFQuery);
		jpConsole.add(bInsert);
		jpConsole.add(bRemove);
		jpConsole.add(bSubscribe);
		jpConsole.add(bUnsubscribe);
		
		/*4_________________________________________________________*/
		//new
		tfSN     = new JTextField("");
		tfPN     = new JTextField("");
		tfON     = new JTextField("");
		tfSNType = new JTextField("");
		tfONType = new JTextField("");
		
		tfSN.setColumns(10);
		tfPN.setColumns(10);
		tfON.setColumns(10);
		tfSNType.setColumns(10);
		tfONType.setColumns(10);
		
		jpTextFieldsNew=new JPanel(false);
		jpTextFieldsNew.add( new JLabel("Subject") );   jpTextFieldsNew.add( tfSN );
		jpTextFieldsNew.add( new JLabel("Predicate") ); jpTextFieldsNew.add( tfPN );
		jpTextFieldsNew.add( new JLabel("Object") );    jpTextFieldsNew.add( tfON );
		jpTextFieldsNew.add( new JLabel("Sub Type") );  jpTextFieldsNew.add( tfSNType );
		jpTextFieldsNew.add( new JLabel("Obj Type") );  jpTextFieldsNew.add( tfONType );
		
		/*5_________________________________________________________*/

		bUpdate    = new JButton("Update");  bUpdate.addActionListener(this);
		bInsertProtection= new JButton("Insert Protection");  bInsertProtection.addActionListener(this);
		bRemoveProtection= new JButton("Remove Protection");  bRemoveProtection.addActionListener(this);
		bInsertProtection.setForeground(Color.RED);
		bRemoveProtection.setForeground(Color.RED);		
		
		jpUpdate=new JPanel(false);
		jpUpdate.add(bUpdate);
		jpUpdate.add(bInsertProtection);
		jpUpdate.add(bRemoveProtection);
		cbSUB=new JComboBox<String>();
		
		/*6_________________________________________________________*/
		
		taMemo=new JTextArea(80,25);
		
		
		/*7___SPARQL________________________________________________*/
		
		bSPARQLquery = new JButton("SPARQL query");
		bSPARQLsubscription = new JButton("SPARQL subscription");
		
		bSPARQLquery.addActionListener(this);
		bSPARQLsubscription.addActionListener(this);
		JLabel labelz = new JLabel("SPARQL query"); labelz.setForeground(Color.RED);
		tfSQ   = new JTextField("");
		tfSQ.setColumns(30);
		
		jpSparql=new JPanel(false);
		jpSparql.add(labelz); 
		jpSparql.add(tfSQ);
		jpSparql.add(bSPARQLquery);
		jpSparql.add(bSPARQLsubscription);
		
		/*8___Subscriptions________________________________________________*/
		jpSubscriptions = new JPanel(false);
		label_sub = new JLabel("Active subscriptions"); labelx.setForeground(Color.BLUE);
		jpSubscriptions.add(label_sub);
		jpSubscriptions.add(cbSUB);
		label_sub = new JLabel("Active subscription"); labelx.setForeground(Color.BLUE);
		
		
		/*_________________________________________________________*/
		
		jpMain=new JPanel(false);
		jpMain.setLayout(new GridLayout(1,1));
		jpMain.add( jpConnection ,0);

		jpMemo=new JPanel(false);
		jpMemo.setLayout(new GridLayout(1,1));
		//ADD HERE A SCROLL BAR PANE
		//jpMemo.add(taMemo);
		JScrollPane sbrText;
		sbrText = new JScrollPane(taMemo);
		sbrText.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		jpMemo.add(  sbrText   );
		
		taMemo.append("OK!");
		
		this.setLayout( new BorderLayout() );
		this.add( jpMain, BorderLayout.SOUTH);
		//this.add( jpMemo, BorderLayout.NORTH );
		
		System.out.println("GUI DONE!");
		
		//this.validateTree();
		
	}//KP_GUI()
	
	/**
	 *  Just a friendly method to print results on the console!
	 */
	private void printResultsOnTaMEMO(Vector<Vector<String[]>> results_vector)
	{
		for (int i = 0; i < results_vector.size(); i++)
		{
			Vector<String[]> single_result_vector = results_vector.elementAt(i);
			this.taMemo.append("\n----- Result "+i+":");
			for (int j = 0; j < single_result_vector.size(); j++) 
			{
				this.taMemo.append("\n--- row "+j+":");
				String[] row = single_result_vector.elementAt(j);
//				System.out.print("\nelement " + j + "=");
				
				for (int k =0; k<row.length; k++ )
				{
					this.taMemo.append(" "+row[k]);
				}
			}
		}
	}
	
	//scroolbars: http://www.roseindia.net/java/java-tips/GUI/components/40textarea/25ex-textarea.shtml
	
	
	
	public void actionPerformed(ActionEvent e)
	  { String ret="";
	    JComponent c = (JComponent) e.getSource();
	    
	    //System.out.println("ACTION : "+comm); if(comm==null)return;
	    //System.out.println("ACTION:"+ (c == bGo?"bGO":"?") );
	    
	    if( c == bGo )
	    {
	    	jpMain.setLayout(new GridLayout(8,1));
	    	System.out.println("ACTION:bGO");
	    	//System.out.println("ACTION:tfIP.getText()"+tfIP.getText());
	    	//System.out.println("ACTION:(String)cbIPList.getSelectedItem()"+(String)cbIPList.getSelectedItem());
	    	System.out.println("ACTION:tfPort:"+tfPort.getText());
	    	
	    	
	    	this.kp=new KPICore( /*tfIP.getText()*/ (String)cbIPList.getSelectedItem()
	    			           ,Integer.parseInt(tfPort.getText())
	    			           ,tfSSN.getText());
	    	
	    	if(!user_kp_id.equals(""))kp.setNodeID(user_kp_id);
	    	
	    	this.kp.enable_debug_message();
	    	this.kp.enable_error_message();
	    	
	    	this.kp.setEventHandler(this);
	    	this.kp.enable_debug_message();
	    	this.kp.enable_error_message();
	    	
	    	if(kp!=null)
	    	{   jpMain.remove(jpConnection);
	    		jpMain.add( jpJoinLeave );
	    		jpMain.add( jpTextFieldsOld );
	    		jpMain.add( jpConsole );
	    		jpMain.add( jpTextFieldsNew );
	    		jpMain.add( jpUpdate );
	    		jpMain.add( jpSparql );
	    		jpMain.add( jpSubscriptions );

	    		this.add( jpMemo, BorderLayout.CENTER );
	    		
	    		taMemo.setText("");
	    		taMemo.append("READY!");
	    		
	    		this.getParent().setSize(900, 400);
	    		this.getParent().repaint();
	    		
		    	xmlTools = new SSAP_XMLTools(null,null,null);

	    	}//if(kp!=null)
	    }//if(c==bGo)

	    else if( c == bSearchRService )
	    {
		    
	    	System.out.println("ACTION:bSearchAndGo");
	    	System.out.println("ACTION:tfSerName:"+tfSerName.getText());
	    	System.out.println("ACTION:cbSOFIADNSIPList:"+cbSOFIADNSIPList.getSelectedItem());

	    	
	    	ArcesServiceRegistry sr=new ArcesServiceRegistry( (String)cbSOFIADNSIPList.getSelectedItem() );
	    	Vector<Properties> reg=null;
	    	Properties userServiceSearch=new Properties();
	    	
	    	//From, e.g. a=1&b=2&qwerty=23, to properties
	    	if(!otherParams.getText().equals(""))
	    	{
		    	String serviceInfo[]=otherParams.getText().split("\\&");
	
		    	for(int i=0;i<serviceInfo.length;i++)
		    	{ String param[]=serviceInfo[i].split("=");  int name=0, value=1;
		    		userServiceSearch.setProperty(param[name], param[value]);
		    	}
	    	}	    	
	    	
	    	reg = sr.search( userServiceSearch );
	    		
	    	if(reg==null || reg.size()==0)
	    	{JOptionPane.showConfirmDialog(null,
			         "No service found!\n...try again!"
					,"WARNING",JOptionPane.CLOSED_OPTION);
	    	}
	    	else
	    	{String str_service_list="";

	    	    for(int i=0; i<reg.size() ;i++)
	    	      { 
	    	    	str_service_list+=reg.elementAt(i).toString()+"\n";
	    	      }
	    	    
	    		JOptionPane.showConfirmDialog(null,
			         "Service found list:\n"
	    			 +str_service_list
	    			 +"\n"
					,"WARNING",JOptionPane.CLOSED_OPTION);
	    	}
	    		
	    		
	    }//else if( c == bSearchRService )
	    
	    else if( c == bJoin)
	    { taMemo.setText(""); 
	      ret = kp.join();  
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
	      taMemo.append("Join confirmed:"+(this.xmlTools.isJoinConfirmed(ret)?"YES":"NO"));
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	    }//bJoin

	    else if( c == bLeave)
	    { taMemo.setText(""); ret = kp.leave(); 
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
	      taMemo.append("Leave confirmed:"+(this.xmlTools.isLeaveConfirmed(ret)?"YES":"NO"));
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	    }//bLeave
	    
	    else if( c == bRDFQuery)
	    { taMemo.setText("");
	      ret = kp.queryRDF( tfSO.getText().equals("*")?null:tfSO.getText()
	    		            ,tfPO.getText().equals("*")?null:tfPO.getText() 
	    		            ,tfOO.getText().equals("*")?null:tfOO.getText()
	    		            ,tfSOType.getText()
	    		            ,tfOOType.getText());
	      
	      
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
	      taMemo.append("RDFQuery confirmed:"+(this.xmlTools.isQueryConfirmed(ret)?"YES":"NO")+"\n");
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	      if(!this.xmlTools.isQueryConfirmed(ret))return;
	      
	      Vector<Vector<String>> triples = null;

	      if(ret!=null)
	      triples = this.xmlTools.getQueryTriple(ret);
	    
	      if(triples!=null)
	      { taMemo.append("Triple List:\n");
	    	for(int i=0; i<triples.size() ; i++ )
		      { Vector<String> t=triples.get(i);
		        String st=  "  S:["+t.get(0)
 	    		           +"] P:["+t.get(1)
 	    		           +"] O:["+t.get(2)
 	    		           +"] Otype:["+t.get(3)+"]";

                taMemo.append(st+"\n");
	    	    
		      }//for(int j=0; i<triple.size() ; i++ )
	      }  
	      
	    }//bRDFQuery
	    else if( c == bSPARQLquery)
	    {
	    	
	    	//TEST 
	    	taMemo.setText("");
	    	taMemo.append("\nSPARQL QUERY!"); 
	    	
	    	ret = kp.querySPARQL(tfSQ.getText());
	    	
	    	taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n");
		    taMemo.append("SPARQLQuery confirmed:"+(this.xmlTools.isQueryConfirmed(ret)?"YES":"NO")+"\n");
		    taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
		    
		    if(ret!=null)
		    {
		    	SSAP_sparql_response myresponse = new SSAP_sparql_response(ret);
		    	taMemo.append("\n-------------------------SPARQL QUERY--------------------------------\n");
				taMemo.append("It is "+myresponse.hasResults()+" that has results\n");
				taMemo.append("Variable names are: "+ myresponse.getVariablesNames()+"\n");
				taMemo.append("---------------------------------------------------------\n");
				taMemo.append("It is "+myresponse.hasLinks()+" that has links\n");
				taMemo.append("Links are: "+myresponse.getLinksHrefs()+"\n");	
				taMemo.append("-------------------------GET SIZE--------------------------------\n");
				taMemo.append("Number of variables = "+myresponse.getVariablesNames().size()+"\n");
				taMemo.append("SIZE (number of results) = "+myresponse.size()+"\n");
				taMemo.append("-------------------------GET RESULTS--------------------------------\n");
				taMemo.append("It is "+myresponse.hasResults()+" that has results\n");
				Vector<Vector<String[]>> results_vector = myresponse.getResults();
				taMemo.append("Results are: ");
				printResultsOnTaMEMO(results_vector);
		    }
		     
	    	
	    }//bSPARQLquery
	    else if( c == bSPARQLsubscription)
	    {
	    	
	    	//TEST 
	    	taMemo.setText("");
	    	taMemo.append("\nSPARQL Subscription!"); 
	    	
	    	ret = kp.subscribeSPARQL(tfSQ.getText());
	    	String subID = xmlTools.getSubscriptionID(ret);
	    	taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n");
		    taMemo.append("SPARQLQuery confirmed:"+(this.xmlTools.isQueryConfirmed(ret)?"YES":"NO")+"\n");
		    taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
		    taMemo.append("\n\n*** subscriptionID:"+subID);
		    cbSUB.addItem(subID);
		    if(ret!=null)
		    {
		    	SSAP_sparql_response myresponse = new SSAP_sparql_response(ret);
		    	taMemo.append("\n-------------------------SPARQL QUERY--------------------------------\n");
				taMemo.append("It is "+myresponse.hasResults()+" that has results\n");
				taMemo.append("Variable names are: "+ myresponse.getVariablesNames()+"\n");
				taMemo.append("---------------------------------------------------------\n");
				taMemo.append("It is "+myresponse.hasLinks()+" that has links\n");
				taMemo.append("Links are: "+myresponse.getLinksHrefs()+"\n");	
				taMemo.append("-------------------------GET SIZE--------------------------------\n");
				taMemo.append("Number of variables = "+myresponse.getVariablesNames().size()+"\n");
				taMemo.append("SIZE (number of results) = "+myresponse.size()+"\n");
				taMemo.append("-------------------------GET RESULTS--------------------------------\n");
				taMemo.append("It is "+myresponse.hasResults()+" that has results\n");
				
				taMemo.append("Results are: ");
				taMemo.append(myresponse.print_as_string());
		    }
		     
	    	
	    }//bSPARQLquery
	    else if( c == bInsert)
	    {
	    	/*String buff = new String(":)");
	    	
	    	for(int i=0;i<15000;i++) buff=buff + "X";
	    	
	    	 taMemo.setText(""); 
	    	 taMemo.append(buff);
	    	 
		      ret = kp.insert(
		    		     tfSO.getText()
			            ,tfPO.getText() 
			            ,buff
			            ,tfSOType.getText()
			            ,tfOOType.getText());
		      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"\n");
	    	  if(true)return;*/
	    	
	     taMemo.setText(""); 
	     ret = kp.insert(
	    		     tfSO.getText()
		            ,tfPO.getText() 
		            ,tfOO.getText()
		            ,tfSOType.getText()
		            ,tfOOType.getText());
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	    }//bInsert

	    else if( c == bRemove)
	    { taMemo.setText("");
	      ret = kp.remove( 
	    		     tfSO.getText().equals("*")?null:tfSO.getText()
	    		    ,tfPO.getText().equals("*")?null:tfPO.getText() 
	    		    ,tfOO.getText().equals("*")?null:tfOO.getText()
		            ,tfSOType.getText()
		            ,tfOOType.getText());	    
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n");
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	    }//bRemove
	 
	    else if( c == bSubscribe)
	    { taMemo.setText("");
	      ret = kp.subscribeRDF(  
	    		             tfSO.getText().equals("*")?null:tfSO.getText()
	    		            ,tfPO.getText().equals("*")?null:tfPO.getText() 
	    		            ,tfOO.getText().equals("*")?null:tfOO.getText()
		            ,tfOOType.getText());	    
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
    	  taMemo.append("Subscribe confirmed:"+(this.xmlTools.isSubscriptionConfirmed(ret)?"YES":"NO")+"\n");	  
    	  my_last_subscription=this.xmlTools.getSubscriptionID(ret);
    	 
    	  cbSUB.addItem(my_last_subscription);
    	  taMemo.append("Subscribe ID:"+my_last_subscription+"\n");	  
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	    }//bSubscribe
	
	    else if( c == bUnsubscribe)
	    { taMemo.setText("");
	      taMemo.append("UnSubscribe ... waiting for the SIB answer...\n");
	      
	      ret = kp.unsubscribe(cbSUB.getSelectedItem().toString());	  
	      cbSUB.removeItemAt(cbSUB.getSelectedIndex());
	      //taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"\n"); 
    	  //taMemo.append("UnSubscribe confirmed:"+(this.xmlTools.isUnSubscriptionConfirmed(ret)?"YES":"NO")+"\n");
	      taMemo.append("...ok:"+ret+"\n");
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	    }//bUnsubscribe
	
	    else if( c == bUpdate)
	    { taMemo.setText("");
	      ret = kp.update( 
	    		     tfSN.getText()
		            ,tfPN.getText() 
		            ,tfON.getText()
		            ,tfSNType.getText()
		            ,tfONType.getText()
	    		    ,tfSO.getText()
		            ,tfPO.getText() 
		            ,tfOO.getText()
		            ,tfSOType.getText()
		            ,tfOOType.getText()
		            );
	      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"\n"); 
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	    }//bUpdate

	    else if( c == bInsertProtection)
	    {boolean success=false;
		 Vector<String> properties = new Vector<String>();
		     
	     taMemo.setText("");

		  if(   tfPO.getText().equals("")||tfSO.getText().equals("")
		     || tfPO.getText().equals("*")||tfSO.getText().equals("*"))
		    { taMemo.append( "ERROR!!!\nSubject or predicate are empty, plese check!!!\n"); 
			  return;
		    }
		  
	     properties.add(tfPO.getText());
	     ret = kp.insertProtection(tfSO.getText(), properties); 
	     success = xmlTools.isInsertConfirmed(ret);
	     
	     taMemo.append( "INSERT PROTECTION:SUCCESS:"+(success?"YES":"NO")
	    		       +"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"\n"); 
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	    }//bInsertProtection

	    else if( c == bRemoveProtection)
	    {boolean success=false;
		 Vector<String> properties = new Vector<String>();
		  taMemo.setText("");
	     
		  if(   tfPO.getText().equals("")||tfSO.getText().equals("")
			 || tfPO.getText().equals("*")||tfSO.getText().equals("*"))
		    { taMemo.append( "ERROR!!!\nSubject or predicate are empty, plese check!!!\n"); 
			  return;
		    }
	      properties.add(tfPO.getText());
		  ret = kp.removeProtection(tfSO.getText(), properties); 
		  success = xmlTools.isRemoveConfirmed(ret);
		     
		  taMemo.append( "REMOVE PROTECTION:SUCCESS:"+(success?"YES":"NO")
	    		        +"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"\n"); 
	      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	      
	    }//bRemove
	    
	    else if( c == bSetKPID )
	    {
	    	//Before...leave the Smart Space...
		      taMemo.setText(""); 
		      ret = kp.leave(); 
		      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
		      taMemo.append("Leave confirmed:"+(this.xmlTools.isLeaveConfirmed(ret)?"YES":"NO"));	    

		      try {Thread.sleep(1000);} catch (InterruptedException e1) {e1.printStackTrace();System.out.print("Sleep failed:"+e1+"\n");}

	    	//set the new KP ID
	    	  kp.setNodeID( tfKPID.getText() );
	    	
	    	//Join the Smart Space taMemo.setText(""); 
              ret = kp.join();  
		      taMemo.append("SIB MESSAGE:\n"+ret+"\n\nKP-CORE MESSAGE:"+kp.getErrMess()+"("+kp.getErrID()+")\n"); 
		      taMemo.append("Join confirmed:"+(this.xmlTools.isJoinConfirmed(ret)?"YES":"NO"));
		      taMemo.append("\n\n*** SSAP message status:"+xmlTools.getSSAPmsgStatus(ret));
	    	
	    }//else if(c==bSetKPID)
	    
	  }//public void actionPerformed(ActionEvent e)

	
	/*___________________________________________________________________*/
	private void memo(String n)
	{
		taMemo.append(n);		
	}
	
	/*____________________________________________________________________*/
	/**
     * Simple implementation of the event handler
     * @param the string representation of the XML event message received from the SIB
     * @see sofia_kp.kp_subscribeHandler#kp_SIBEventHandler(java.lang.String)
     */
	@Override
//*****************************************************************************
	public void kpic_SIBEventHandler(final String xml)
	{
		//xml_i = xml;
		//k++;
		
		// TODO Auto-generated method stub
		new Thread(
				new Runnable() {
					public void run() {
						String id = "";
						String k = "";
						SSAP_XMLTools xmlTools = new SSAP_XMLTools();
						id = xmlTools.getSubscriptionID(xml);
						//k = xmlTools.get
						System.out.println (xml);
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
									                	memo(temp);
						}
						else
						{
							System.out.println("Notif. " + k + " id = " + id +"\n");
							memo("Notif. " + k + " id = " + id +"\n");
							SSAP_sparql_response resp_new = xmlTools.get_SPARQL_indication_new_results(xml);
							SSAP_sparql_response resp_old = xmlTools.get_SPARQL_indication_obsolete_results(xml);
							if (resp_new != null)
							{
								System.out.println("new: \n " + resp_new.print_as_string());
								memo("new: \n " + resp_new.print_as_string());
							}
							if (resp_old != null)
							{
								System.out.println("obsolete: \n " + resp_old.print_as_string());
								memo("obsolete: \n " + resp_old.print_as_string());
							}
						}
						
					}
				}).start();
	}
//***************************************************************************	
//	public void kpic_SIBEventHandler(String xml)
//    { SSAP_XMLTools kp=new SSAP_XMLTools(null,null,null);
//      
//      System.out.println("\n[EVENT (KP_GUI)]___________________________________");
//      System.out.println("EVENT:\n"+xml); 
//      memo("\n[EVENT]___________________________________\n"+xml+"\n");
//
//      if( this.xmlTools.isUnSubscriptionConfirmed(xml) )
//      { taMemo.append("UnSubscribe confirmed:"+"YES"+"\n");
//        return;
//      }//if( this.xmlTools.isUnSubscriptionConfirmed(xml) )
//
//      
//      Vector<Vector<String>> triples = this.xmlTools.getNewResultEventTriple(xml);
//	    
//      if(triples!=null)
//      { taMemo.append("NEW RESULT:\n");
//    	for(int i=0; i<triples.size() ; i++ )
//	      { Vector<String> t=triples.get(i);
//	        String st=  "  S:["+t.get(0)
//	    		           +"] P:["+t.get(1)
//	    		           +"] O:["+t.get(2)
//	    		           +"] Otype:["+t.get(3)+"]";
//
//            taMemo.append(st+"\n");
//    	    
//	      }//for(int j=0; i<triple.size() ; i++ )
//      }//if(triples!=null)  
//
//      triples = this.xmlTools.getObsoleteResultEventTriple(xml);
//	    
//      if(triples!=null)
//      { taMemo.append("OBSOLETE RESULT:\n");
//    	for(int i=0; i<triples.size() ; i++ )
//	      { Vector<String> t=triples.get(i);
//	        String st=  "  S:["+t.get(0)
//	    		           +"] P:["+t.get(1)
//	    		           +"] O:["+t.get(2)
//	    		           +"] Otype:["+t.get(3)+"]";
//
//            taMemo.append(st+"\n");
//    	    
//	      }//for(int j=0; i<triple.size() ; i++ )
//      }//if(triples!=null)  
//      
//
//    }//public void kp_sib_event(String xml)


    
    /*____________________________________________________________________*/
    public static void main(String args[])
    {    	 
    	String ip="127.0.0.1",
    	       port="10010",
    	       ssname="X",
    	       s="*",
    	       p="*",
    	       o="*",
    	       kpid="DEAD-BEAF",
    	       def_kpid="DEAD-BEAF",
    	       autoJoin="false",
    	       goChanges="false";
    	
    	System.out.println("\n**************************************************");
    	System.out.println("*  Available parameters (with default values):");
    	System.out.println("*      -ip "+ip);
        System.out.println("*      -port "+port);
        System.out.println("*      -ssname "+ssname);
    	System.out.println("*      -s "+s);
    	System.out.println("*      -p "+p);
    	System.out.println("*      -o "+o);
    	System.out.println("*      -kpid "+"<random UUID>");
    	System.out.println("*      -autoJoin "+autoJoin+" [true|false]");
    	System.out.println("**************************************************\n\n");
    	
    	if (args.length > 0) 
    	for(int i=0; i<args.length ;i++)
    	{
                 if( args[i].equals("-ip") ) {ip=args[++i];  goChanges="true";}
            else if( args[i].equals("-port") ) {port=args[++i]; goChanges="true";}
            else if( args[i].equals("-ssname") ) {ssname=args[++i]; goChanges="true";}
            else if( args[i].equals("-s") ) {s=args[++i];}
            else if( args[i].equals("-p") ) {p=args[++i];}
            else if( args[i].equals("-o") ) {o=args[++i];}
            else if( args[i].equals("-kpid") ) {kpid=args[++i];}
            else if( args[i].equals("-autoJoin") ) {autoJoin=args[++i];}
            else System.out.println("Parameter not recognized: "+args[i]);     
    	}//if (args.length > 0)

    	
        KP_GUI kpgui = new KP_GUI();
        Frame   f = new Frame(" - SOFIA KP - ");

        
        f.addWindowListener(new WindowAdapter() {
          public void windowClosing(WindowEvent e) {
          System.exit(0);}});

        f.setSize(new Dimension(700,200));
        f.add("Center", kpgui);        //
       // f.pack();
        f.setVisible(true);
        
        /*______________________*/
        //kpgui.tfIP.setText("127.0.0.1");        
        //kpgui.cbIPList.setSelectedItem("mml.arces.unibo.it");
        kpgui.tfIP.setText(ip);        
        kpgui.cbIPList.setSelectedItem(ip);
        
        
        kpgui.tfPort.setText(port);
        kpgui.tfSSN.setText(ssname);
        
/** /
        kpgui.tfSO.setText("*");
        kpgui.tfPO.setText("*");
        kpgui.tfOO.setText("*");
/ **/
        kpgui.tfSO.setText(s);
        kpgui.tfPO.setText(p);
        kpgui.tfOO.setText(o);
/**/        
        kpgui.tfSOType.setText("uri");
        kpgui.tfSNType.setText("uri");
        kpgui.tfOOType.setText("uri");
        kpgui.tfONType.setText("uri");
/**/
        kpgui.tfKPID.setText(kpid);
        /*______________________*/

        
    	if(!kpid.equals(def_kpid))kpgui.user_kp_id=kpid;
    	if(goChanges.equals("true"))
    		 kpgui.actionPerformed( new ActionEvent( kpgui.bGo,0,null) );
    	if(autoJoin.equals("true"))
    		 { if(!goChanges.equals("true"))
    			 kpgui.actionPerformed( new ActionEvent( kpgui.bGo,0,null) );
    		   kpgui.actionPerformed( new ActionEvent( kpgui.bJoin,0,null) );
    		 }
    	
    	kpgui.taMemo.append("\n**************************************************\n");
    	kpgui.taMemo.append("*  KP parameters:\n");
    	kpgui.taMemo.append("*      -ip "+ip+"\n");
        kpgui.taMemo.append("*      -port "+port+"\n");
        kpgui.taMemo.append("*      -ssname "+ssname+"\n");
    	kpgui.taMemo.append("*      -s "+s+"\n");
    	kpgui.taMemo.append("*      -p "+p+"\n");
    	kpgui.taMemo.append("*      -o "+o+"\n");
    	if(goChanges.equals("true"))
    	kpgui.taMemo.append("*      -kpid "+kpgui.kp.nodeID+"\n");
    	kpgui.taMemo.append("**************************************************\n");

    	
    }//public static void main(String args[])



    
}
