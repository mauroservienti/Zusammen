// Messages are plain classes; the class name is the message type ("OrderPlaced")
export class OrderPlaced {
  orderId: string;
  total: number;

  constructor(orderId: string, total: number) {
    this.orderId = orderId;
    this.total = total;
  }
}
