
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
	
		//Recieve OK 
		String serverMessage = in.readLine();
	
		//Send AUTH
		String username= System.getProperty("user.name");
		out.write(("AUTH"+username+"\n").getBytes());
	
		//Recieve OK
		serverMessage = in.readLine();
		int numOfLargServers=0;
        String serverType ="";

		//Send REDY for first job 
		out.write("REDY\n".getBytes());
		String jobState = in.readLine();

        //Sent GETS message
		out.write(("GETS All\n").getBytes()); 
		//Recieve DATA nRecs recSize
		serverMessage = in.readLine();
				
		//extract nRecs
		String [] arrOfMess = serverMessage.split(" ");
		int nRecs = Integer.valueOf(arrOfMess[1]);
					
		//Send OK
		out.write(("OK\n").getBytes());
					
					
		//Recieve each record of servers
		String [] servers = new String[nRecs]; 
		int biggestCore = 0;
		int serverCores=0;
				

		for(int i=0;i<nRecs;i++){
			servers[i] = in.readLine();	
			String [] serverInfo = servers[i].split(" ");					
			serverCores = Integer.valueOf(serverInfo[4]);

			if(serverCores>=biggestCore){
				biggestCore=serverCores;
				serverType = serverInfo[0];
				}

		}

				
		// System.out.println("Largest no. of Cores: "+biggestCore);
		// System.out.println("Server Type: "+serverType);

		//finding the number of servers of serverType
		for(int i=0; i<nRecs;i++){
			String [] serverInfo = servers[i].split(" ");
			String typeTemp = serverInfo[0];
			if(serverType.equals(typeTemp)){
				numOfLargServers++;
			}
		}

		//Ready to 
		out.write("OK\n".getBytes());
		in.readLine();

		int serverCounter=0;
		boolean initialJob=true;

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

				if(!initialJob){
					//Send OK
					out.write(("OK\n").getBytes());
					//System.out.println("sending OK");
				
					//Recieve .
					serverMessage = in.readLine();
					//System.out.println("Server says: "+serverMessage);
				}
				
				initialJob=false;
				//SCHDuling the JOBN
				System.out.println("Scheduling job:SCHD "+jobID+" "+serverType+" "+serverCounter+"\n");
				out.write(("SCHD "+jobID+" "+serverType+" "+serverCounter+"\n").getBytes());
				serverCounter++;
				serverMessage=in.readLine();
				System.out.println("Server says: "+serverMessage);



			} 
			else{
				//if JCLP do nothing
			}
			
			//REDY for next job and reading in
			out.write("REDY\n".getBytes());
			jobState = in.readLine();
			System.out.println("job is: "+jobState);

		}
		

		//Send QUIT
		out.write(("QUIT\n").getBytes());
		
		//Recieve QUIT
	 	in.readLine();
		System.out.println("Goodbye and thank you for using this job scheduler :)");
	
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
