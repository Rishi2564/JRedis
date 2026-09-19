package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Service.CommandHandler;
import Components.Service.ResponseDto;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Service.ResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class SlaveTcpServer {
    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;

    @Autowired
    private ConnectionPool connectionPool;
    public void startServer() {
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = redisConfig.getPort();
//        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            CompletableFuture<Void> slaveConnectionFuture = CompletableFuture.runAsync(this::initiateSlavery);
            slaveConnectionFuture.thenRun(()->System.out.println("Replication completed"));
            int id=0;
            while(true){
                clientSocket = serverSocket.accept();
                id++;
                Socket finalClientSocket = clientSocket;
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();
                Client client= new Client(finalClientSocket,inputStream,outputStream,id);
                CompletableFuture.runAsync(() -> {
                    try{
                        handleClient(client);
                    }catch (Exception e){
                        throw new RuntimeException(e);
                    }

                });
            }



        } catch (IOException e) {
            log.error("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                log.error("IOException: " + e.getMessage());
            }
        }

    }

    private void initiateSlavery() {
        try(Socket master= new Socket(redisConfig.getMasterHost(), redisConfig.getMasterPort())){
            InputStream inputStream = master.getInputStream();
            OutputStream outputStream = master.getOutputStream();
            byte[] inputBuffer = new byte[1024];
            byte[] data = "*1\r\n$4\r\nPING\r\n". getBytes();
            outputStream.write(data);
            int bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            String response=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log.info("Master response: {}",response);

            int lenListeningPort=(redisConfig.getPort()+"").length();
            int listeningPort=redisConfig.getPort();
            String replconf="*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"+(lenListeningPort+"")+"\r\n"+(listeningPort+"")+"\r\n";
            data = replconf.getBytes();
            outputStream.write(data);
            bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            response=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log.info("Master response: {}",response);

            replconf="*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            data = replconf.getBytes();
            outputStream.write(data);
            bytesRead = inputStream.read(inputBuffer,0,inputBuffer.length);
            response=new String(inputBuffer,0,bytesRead, StandardCharsets.UTF_8);
            log.info("Master response: {}",response);

            String psync="*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            data = psync.getBytes();
            outputStream.write(data);
            List<Integer> res= handlePsyncResponse(inputStream);

            while(master.isConnected()){
                int offset=1;
                StringBuilder sb=new StringBuilder();
                List<Byte> bytes=new ArrayList<>();
                while(true){
                    int b=inputStream.read();
                    if(b=='*'){
                        break;
                    }
                    offset++;
                    bytes.add((byte)b);
                    if(inputStream.available()<=0){
                        break;
                    }
                }

                for(Byte b:bytes){
                    sb.append((char) (b.byteValue()&0xFF));
                }
                if(bytes.isEmpty())
                    continue;
                String command=sb.toString();
                String parts[]=command.split("\r\n");
                if(command.equals("+OK\r\n"))
                    continue;
                String[] commandArray=respSerializer.parseArray(parts);
                Client masterClient=new Client(master,master.getInputStream(),master.getOutputStream(),-1);
                String commandResult=handleCommandFromMaster(commandArray, masterClient);
                if(commandArray.length>=2&&commandArray[0].equals("REPLCONF")&&commandArray[1].equals("GETACK")){
                    if(!commandResult.equals("")&&commandResult!=null)
                        outputStream.write(commandResult.getBytes());
                    offset++;
                    List<Byte> leftOverBytes=new ArrayList<>();
                    while(true){
                        if(inputStream.available()<=0){
                            break;
                        }
                        byte b=(byte)inputStream.read();
                        leftOverBytes.add(b);
                        if((int)b==(int)'*')break;
                        offset++;
                    }
                    StringBuilder leftOverSb=new StringBuilder();
                    for(Byte b:leftOverBytes){
                        leftOverSb.append((char)(b.byteValue()&0xFF));
                    }

                }
                redisConfig.setMasterReplOffset(offset+redisConfig.getMasterReplOffset());
            }

        }catch (Exception e){
            log.error("Failed to connect to master",e);
        }
    }

    private String handleCommandFromMaster(String[] command, Client master) {
        String cmd=command[0];
        cmd=cmd.toUpperCase();
        String res="";
        switch (cmd){
            case "SET":
                commandHandler.set(command);
                CompletableFuture.runAsync(()->propagate(command));
                break;
            case "REPLCONF":
                res=commandHandler.replconf(command,master);
                break;
        }
        return res;
    }
    private void propagate(String[] command) {
        String commandRespString=respSerializer.respArray(command);
        try{
            for(Slave slave:connectionPool.getSlaves()){
                slave.send(commandRespString.getBytes());
            }
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
    private List<Integer> handlePsyncResponse(InputStream inputStream) throws IOException {
        List<Integer> res= new ArrayList<>();
        while(true){
            if(inputStream.available()<=0)
                continue;
            int b=inputStream.read();
            res.add(b);
            if(b==(int)'*'){
                break;
            }
        }
        return res;
    }

    public void handleClient(Client client)throws IOException {
        connectionPool.addClient(client);
        while(client.socket.isConnected()){
            byte[] buffer = new byte[client.socket.getReceiveBufferSize()];
            int bytesRead=client.inputStream.read(buffer);
            if(bytesRead>0){
                List<String[]> commands=respSerializer.deserialize(buffer);
                for(String[] command:commands){
                    handleCommand(command,client);
                }
            }
        }
        connectionPool.removeClient(client);
        connectionPool.removeSlave(client);
    }
    public void handleCommand(String[] command,Client client)throws IOException {
        String res="";
        byte[] data=null;
        switch (command[0]){
            case "PING":
                res=commandHandler.ping(command);
                break;

            case "ECHO":
                res=commandHandler.echo(command);
                break;
            case "SET":
                res="-READONLY You can't write against a replica. \r\n";
                break;
            case "GET":
                res=commandHandler.get(command);
                break;
            case "INFO":
                res=commandHandler.info(command);
                break;
            case "REPLCONF":
                res=commandHandler.replconf(command, client);
                break;
            case "PSYNC":
                ResponseDto resDto  =commandHandler.psync(command);
                res=resDto.response;
                data=resDto.data;
                break;
        }
        client.send(res,data);
    }
}
