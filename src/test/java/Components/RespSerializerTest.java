package Components;


import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RespSerializerTest {
private final RespSerializer respSerializer = new RespSerializer();
@Test
    public void testDeserializePing(){
    String ping="*1\r\n$4\r\nPING\r\n";
    List<String[]> commands= respSerializer.deserialize(ping.getBytes(StandardCharsets.UTF_8));
    for(String[] s: commands){
        System.out.println();
        for(String ss: s){
            System.out.println(ss+" ");
        }
    }
    assertEquals(1,commands.size());
    assertEquals(1,commands.get(0).length);
    assertEquals("PING",commands.get(0)[0]);
}
@Test
    public void testMultipleCommands(){
    String multipleCommands="*2\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue";
    List<String[]> commands= respSerializer.deserialize(multipleCommands.getBytes(StandardCharsets.UTF_8));
    for(String[] s: commands){
        System.out.println();
        for(String ss: s){
            System.out.println(ss+" ");
        }
    }
}
}