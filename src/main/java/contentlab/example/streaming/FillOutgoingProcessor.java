package contentlab.example.streaming;

import contentlab.example.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@EnableScheduling
public class FillOutgoingProcessor {
  private Logger logger = LoggerFactory.getLogger(FillOutgoingProcessor.class);
  private Socket clientSocket;
  private BufferedWriter bufferedWriter;
  private String[] symbolArray = {"IBM", "MSFT", "GOOGL", "F", "YHOO"};
  private Random random = new Random();
  private List<Trade> tradeList = new ArrayList<Trade>();

  @PostConstruct
  public void initialize() {
    while(true) {
      try {
        clientSocket = new Socket("localhost", 19999);
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        break;
      } catch (Exception ex) {
        try { Thread.sleep(1000); } catch(Exception sleepEx) { }
      }
    }

    for(int tradeCount = 0; tradeCount < 5; tradeCount++) {
      String symbol = symbolArray[tradeCount];
      int quantity = 10000;
      double price = Math.round(random.nextDouble() * 1000.0) / 100.0;
      tradeList.add(new Trade(symbol, quantity, price));
    }

  }

  @Scheduled(initialDelay = 5000, fixedDelay = 5000)
  public void sendOutgoingFills() {
    try {
      Trade trade = tradeList.get(random.nextInt(5));
      if (!trade.isFilled()) {
        int fillQuantity = random.nextInt(1000);
        fillQuantity = trade.applyFill(fillQuantity);
        double price = Math.round(random.nextDouble() * 1000.0) / 100.0;
        String json = trade.getJson(fillQuantity, price);
        logger.info("Sending Outgoing Fill: " + json);
        //bufferedWriter.write(json + "\n");
        bufferedWriter.flush();
      }
    }
    catch(Exception ex) {
      logger.error("Error Sending Fill", ex);
    }
  }
}
