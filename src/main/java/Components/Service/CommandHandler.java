package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Server.RedisConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Base64;

@Slf4j
@Component
public class CommandHandler {
    private static final String emptyRdbFile="UkVESVМwМDЕx+gyZWRpcy12ZХIFNy4yLjD6CnJLZGLzLWJpdHPАQPoFY3RpbWXCbQi8Zf0IdXNLZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    @Autowired
    public RespSerializer respSerializer;
    @Autowired
    public Store store;
    @Autowired
    public RedisConfig redisConfig;
    @Autowired
    public ConnectionPool connectionPool;
    public String ping(String[] command){
        return "+PONG\r\n";
    }

    public String echo(String[] command) {
        return respSerializer.serializeBulkString(command[1]);
    }

    public String set(String[] command) {
        try{
            String key=command[1];
            String value=command[2];
            int pxFlag= Arrays.stream(command).toList().indexOf("px");
            if(pxFlag!=-1){
                int delta=Integer.parseInt(command[pxFlag+1]);
                return store.set(key, value,delta);
            }else {
                return store.set(key, value);
            }
        }
        catch(Exception e){
            log.error(e.getMessage());
            return "$-1\r\n";
        }

    }
    public String get(String[] command) {
        try {
            String key = command[1];
            return store.get(key);
        }catch(Exception e){
            log.error(e.getMessage());
            return "$-1\r\n";
        }
    }
    public String info(String[] command) {
        int replication=Arrays.stream(command).toList().indexOf("replication");
        if(replication>-1){
            String role="role:"+redisConfig.getRole();
            String masterReplId="master_replid:"+redisConfig.getMasterReplId();
            String masterReplOffset="master_repl_offset:"+redisConfig.getMasterReplOffset();
            String []info=new String[]{role,masterReplId,masterReplOffset};
            String replicationData= String.join("\r\n",info);
            return respSerializer.serializeBulkString(replicationData);
        }
        return "$-1\r\n";
    }

    public String replconf(String[] command, Client client) {
        switch(command[1]){
            case "GETACK":
                String[] replconfAck=new String[]{"REPLCONF","ACK",redisConfig.getMasterReplOffset()+""};
                return respSerializer.respArray(replconfAck);
               
            case "listening-port":
                connectionPool.removeClient(client);
                Slave s=new Slave(client);
                connectionPool.addSlave(s);
                return "+OK\r\n";
                
            case "capa":
                Slave slave=null;
                for(Slave ss:connectionPool.getSlaves()){
                    if(ss.connection.equals(client)){
                        slave=ss;
                        break;
                    }
                }
                for(int i=0;i<command.length;i++){
                    if(command[i].equals("capa")){
                        slave.capabilities.add(command[i+1]);
                    }
                }
                return "+OK\r\n";

        }
        return "+OK\r\n";
    }

    public byte[] concatenate(byte[] a , byte[] b){
        byte[] result=new byte[a.length+b.length];
        System.arraycopy(a,0,result,0,a.length);
        System.arraycopy(b,0,result,a.length,b.length);
        return result;
    }
    public ResponseDto psync(String[] command) {
        String replicationIdMaster=command[1];
        String replicationOffsetMaster=command[2];
        if(replicationIdMaster.equals("?")&&replicationOffsetMaster.equals("-1")){
            String replicationId=redisConfig.getMasterReplId();
            Long replicationOffset=redisConfig.getMasterReplOffset();
            String res="+FULLRESYNC "+replicationId +" "+replicationOffset+"\r\n";
            byte[] rdbFileData= Base64.getDecoder().decode(emptyRdbFile);
            String length=rdbFileData.length+"";
            String fullResyncHeader="$"+length+"\r\n";
            byte[] header=fullResyncHeader.getBytes();

            return new ResponseDto(res,concatenate(header,rdbFileData));
        }else{
            return new ResponseDto("Options not supported yet.");
        }
    }
}
