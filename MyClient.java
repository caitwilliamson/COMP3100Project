
import java.net.*;  
import java.io.*;  

class MyClient{  
public static void main(String args[]){  

	//Create a socket
	Socket s=null;


	try{
	
		//Connect to ds-server
		int serverPort = 50000;
		s=new Socket("localhost", serverPort); 
		//System.out.println("Port number: "+s.getPort());  
		
		//Intialise input and output streams associated with socket
		BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
		DataOutputStream out=new DataOutputStream(s.getOutputStream());  



		//HANDSHAKE
		//Send HELO
		out.write(("HELO\n").getBytes()); 
		//System.out.println("Sent: HELO");
	
		//Recieve OK 
		String serverMessage = in.readLine();
		//System.out.println("Recieved: "+serverMessage);
	
		//Send AUTH
		String username= System.getProperty("user.name");
		out.write(("AUTH"+username+"\n").getBytes());
	
		//Recieve OK
		serverMessage = in.readLine();
		//System.out.println("Recieved: "+serverMessage);
		int numOfLargServers=1;
        String serverType ="";

		//Send REDY for first job (will be asked again)
		out.write("REDY\n".getBytes());
		String jobState = in.readLine();

        //Sent GETS message
				out.write(("GETS All\n").getBytes()); 
				//Recieve DATA nRecs recSize
				serverMessage = in.readLine();
				System.out.println("GETS is: "+serverMessage);
				
				//extract nRecs
				String [] arrOfMess = serverMessage.split(" ");
				int nRecs = Integer.valueOf(arrOfMess[1]);
					
				//Send OK
				out.write(("OK\n").getBytes());
					
					
				//Recieve each record of servers
				String [] servers = new String[nRecs]; 
				

				int biggestCore = 0;
				int serverID=0;
				int serverCores=0;
				

				for(int i=0;i<nRecs;i++){
					servers[i] = in.readLine();	
					//System.out.println(servers[i]);
					String [] serverInfo = servers[i].split(" ");
					//System.out.println(Arrays.toString(serverInfo));						
					serverCores = Integer.valueOf(serverInfo[4]);

						if(serverCores>=biggestCore){
							biggestCore=serverCores;
							//System.out.println("Largest no. of Cores: " + serverCores);
							serverType = serverInfo[0];
							//System.out.println("Server Type: "+serverType);
							serverID = Integer.valueOf(serverInfo[1]);
							//System.out.println("Server ID: "+serverID);
						}

				}

				
				System.out.println("Largest no. of Cores: "+biggestCore);
				System.out.println("Server Type: "+serverType);
				System.out.println("Server ID: "+serverID);

				numOfLargServers=0;
				for(int i=0; i<nRecs;i++){
					//System.out.println("entering");
					String [] serverInfo = servers[i].split(" ");
					String typeTemp = serverInfo[0];
					//System.out.println("comparing "+typeTemp+" and "+serverType);
					if(serverType.equals(typeTemp)){
						//System.out.println("iterating");
						numOfLargServers++;
					}
				}

		System.out.println("number of large servers:"+numOfLargServers);
		out.write("OK\n".getBytes());
		in.readLine();

		int serverCounter=0;


		//While last message is not NONE
		while(!jobState.equals("NONE")){

			System.out.println("Job is: "+jobState);
			String [] jobInfo = jobState.split(" ");
			String jobCommand = jobInfo[0];


			if(jobCommand.equals("JOBN")){
				int jobID = Integer.valueOf(jobInfo[2]);
                //Round-Robin
                if(serverCounter>=numOfLargServers){
                    serverCounter=0;
                }

				//Send OK
				out.write(("OK\n").getBytes());
				//System.out.println("sending OK");
				
				
				//Recieve .
				serverMessage = in.readLine();
				//System.out.println("Server says: "+serverMessage);
			
				//SCHDuling the JOBN
				System.out.println("Scheduling job:SCHD "+jobID+" "+serverType+" "+serverCounter+"\n");
				out.write(("SCHD "+jobID+" "+serverType+" "+serverCounter+"\n").getBytes());
				serverCounter++;
				serverMessage=in.readLine();
				System.out.println("Server says: "+serverMessage);



			} 
			else{

			}
			
			out.write("REDY\n".getBytes());
			jobState = in.readLine();
			System.out.println("job is: "+jobState);

		}
		

		//Send QUIT
		out.write(("QUIT\n").getBytes());
		
		//Recieve QUIT
	 	in.readLine();
	
	 	//Close the socket
		in.close();
		out.close();
	 	s.close();
		

	}

	


	catch (UnknownHostException e){
		System.out.println("Sock: "+e.getMessage());
	}
	catch(EOFException e){
		System.out.println("EOF: "+e.getMessage());
	}
	catch(IOException e){
		System.out.println("IO: "+e.getMessage());
	}
	finally {if (s!=null){
		try { s.close();
		}
		catch(IOException e){
			System.out.println("close: "+e.getMessage());
		}
	}}}
}
