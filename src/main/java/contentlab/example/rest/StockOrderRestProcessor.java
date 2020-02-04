package contentlab.example.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

@RestController
public class StockOrderRestProcessor {
  @Autowired Connection postgresConnection;

  @GetMapping(value = "/report")
  @ResponseBody
  public ResponseEntity<String> getFillReport() {
    try {
      String report = report();
      return new ResponseEntity<String>(report, HttpStatus.OK);
    }
    catch(Exception ex) {
      return new ResponseEntity<String>("Error Getting Report: " + ex, HttpStatus.BAD_REQUEST);
    }
  }

  @GetMapping(value = "/report/{symbol}")
  @ResponseBody
  public ResponseEntity<String> getFillReportBytSymbol(@PathVariable(name = "symbol", required = true) String symbol) {

    try {
      String report = report(symbol);
      return new ResponseEntity<String>(report, HttpStatus.OK);
    }
    catch(Exception ex) {
      return new ResponseEntity<String>("Error Getting Report: " + ex, HttpStatus.BAD_REQUEST);
    }
  }

  private String report() throws Exception {
    return report(null);
  }

  private String report(String symbol) throws Exception {
    PreparedStatement statement = null;
    ResultSet rs = null;
    StringBuffer html = new StringBuffer("<html>");
    html.append("<table border=\"1\">");
    html.append("<tr><th>Symbol</th><th>Shares Filled</th><th>Original Quantity</th><th>Average Filled Price</th><th>Original Trade Price</th></tr>");
    try {
      String sql = "SELECT \n" +
        "  fill->>'Symbol' AS Symbol,\n" +
        "  SUM(CAST(fill->>'SharesFilled' AS INTEGER)) AS SharesFilled,\n" +
        "  AVG(CAST(fill->'OriginalOrder'->>'Quantity' AS INTEGER)) AS OriginalQuantity,\n" +
        "  SUM(CAST(fill->>'SharesFilled' AS INTEGER) * CAST(fill->>'Price' AS NUMERIC)) /\n" +
        "      SUM(CAST(fill->>'SharesFilled' AS INTEGER)) AS AverageFilledPrice,\n" +
        "  AVG(CAST(fill->'OriginalOrder'->>'TradePrice' AS NUMERIC)) AS OriginalTradePrice\n" +
        "  FROM stockfills";
      if (symbol != null)
        sql += " where fill->>'Symbol' = ? GROUP BY Symbol";
      else
        sql += " GROUP BY Symbol";

      statement = postgresConnection.prepareStatement(sql);

      if (symbol != null)
        statement.setString(1, symbol);
      rs = statement.executeQuery();
      while(rs.next()) {
        html.append("<tr>").
          append("<td>").append(rs.getString("Symbol")).append("</td>").
          append("<td>").append(rs.getInt("SharesFilled")).append("</td>").
          append("<td>").append(rs.getInt("OriginalQuantity")).append("</td>").
          append("<td>").append(rs.getBigDecimal("AverageFilledPrice").setScale(2, RoundingMode.HALF_UP)).append("</td>").
          append("<td>").append(rs.getBigDecimal("OriginalTradePrice").setScale(2, RoundingMode.HALF_UP)).append("</td>").
          append("</tr>");
      }
      html.append("</table>");
      html.append("</html>");
      return html.toString();
    }
    finally {
      try { rs.close(); } catch (Exception ex) { }
      try { statement.close(); } catch(Exception ex) { }
    }
  }
}
