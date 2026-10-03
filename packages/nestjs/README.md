# @zusammen/nestjs

NestJS 12 integration for [Zusammen](https://github.com/mauroservienti/Zusammen) transactional sessions.

```typescript
import { CurrentSession, SessionAccessor, Transactional, ZusammenModule } from '@zusammen/nestjs';

@Module({
  imports: [ZusammenModule.forRoot({ factory })],
  controllers: [OrdersController],
  providers: [OrderService],
})
class AppModule {}

@Controller('orders')
class OrdersController {
  constructor(@Inject(OrderService) private readonly orders: OrderService) {}

  @Post()
  @Transactional()
  async place(@Body() order: Order, @CurrentSession() session: TransactionalSession<ClientSession>) {
    await this.orders.place(order, session);
    return { id: order.id };
  }
}
```

Routes opt in with `@Transactional()` on the handler or controller, or use `ZusammenModule.forRoot({ factory, global: true })`. The interceptor commits after the handler returns and before Nest sends the response; exceptions roll back, and a failed commit becomes a 500. Services can reach the session through `SessionAccessor.current` or `getSession()` from `@zusammen/core`.

The module starts the factory on module init and stops it on application shutdown (`manageLifecycle: false` to manage it yourself; enable Nest's shutdown hooks). Use `ZusammenModule.forRootAsync({ useFactory, inject, imports })` to build the factory from other providers.

Handlers that write the response themselves with `@Res()` aren't supported.
