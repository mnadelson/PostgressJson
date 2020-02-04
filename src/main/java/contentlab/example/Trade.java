package contentlab.example;

public class Trade {
  private String symbol;
  private int originalQuantity;
  private int quantity;
  private double tradePrice;

  public Trade(String symbol, int quantity, double tradePrice) {
    this.symbol = symbol;
    this.originalQuantity = quantity;
    this.quantity = quantity;
    this.tradePrice = tradePrice;
  }

  public boolean isFilled() {
    return quantity == 0;
  }

  public int applyFill(int fillQuantity) {
    if (fillQuantity > quantity)
      fillQuantity = quantity;

    quantity -= fillQuantity;
    return fillQuantity;
  }

  public String getJson(int fillQuantity, double price) {
    return "{\"Symbol\":\""+symbol+"\", \"SharesFilled\":"+fillQuantity+", \"Price\":"+price+", \"OriginalOrder\": {\"Quantity\":"+originalQuantity+", \"TradePrice\":"+tradePrice+"}}";
  }
}
