// Mapped to its .NET type in api.ts: carries NServiceBus.EnclosedMessageTypes
export class OrderPlaced {
  orderId: string;

  constructor(orderId: string) {
    this.orderId = orderId;
  }
}

// Not mapped: the Billing endpoint resolves "ChargeCustomer" with Zusammen.NServiceBus
export class ChargeCustomer {
  orderId: string;
  amount: number;

  constructor(orderId: string, amount: number) {
    this.orderId = orderId;
    this.amount = amount;
  }
}
