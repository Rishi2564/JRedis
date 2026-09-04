import Components.TcpServer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    AnnotationConfigApplicationContext context= new AnnotationConfigApplicationContext(AppConfig.class);
    TcpServer app= context.getBean(TcpServer.class);
    app.startServer();
    //  Uncomment the code below to pass the first stage
  }
}
