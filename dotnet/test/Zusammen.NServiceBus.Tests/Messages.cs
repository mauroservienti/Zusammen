namespace Sales
{
    public class OrderPlaced : NServiceBus.IEvent;

    public class PlaceOrder : NServiceBus.ICommand;
}

namespace Billing
{
    public class OrderPlaced : NServiceBus.IEvent;
}
