package contentlab.example.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.net.SocketFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Service
@EnableScheduling
public class FillIncomingProcessor {
//  static {
//    try { Class.forName("org.postgresql.Driver"); } catch(Exception ex) { ex.printStackTrace(); }
//  }
  private Logger logger = LoggerFactory.getLogger(FillIncomingProcessor.class);
  private BufferedReader reader;

  @PostConstruct
  public void initialize() throws IOException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    ServerSocket serverSocket = new ServerSocket(19999);
    new Thread() {
      public void run() {
        try {
          Socket socket = serverSocket.accept();
          reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
          while(true) {
            try {
              String json = reader.readLine();
              logger.info("Received Fill: " + json);
              persistFill(json);
            }
            catch(Exception ex) {
              logger.error("Error processing incoming fill message",ex);
            }
          }
        }
        catch(Exception ex) {
          logger.error("Error Accepting Connection", ex);
        }
      }
    }.start();
  }

  @Bean
  private Connection postgresConnection() throws SQLException {
    String url = "jdbc:postgresql://localhost:9999/test?user=postgres";
    return DriverManager.getConnection(url);
  }

  private void persistFill(String json) throws SQLException {
    PreparedStatement preparedStatement = postgresConnection().prepareStatement("insert into StockFills(fill) values (?::JSON)");
    preparedStatement.setObject(1, json);
    preparedStatement.executeUpdate();
    preparedStatement.close();
  }
}
