import { Actae } from '../src/index.js';

const actae = Actae.fromEnv();

const createReceipt = actae.effect(
  { key: 'order:{orderId}:receipt', name: 'create_receipt' },
  async ({ orderId, amount }: { orderId: string; amount: number }) => ({
    receiptId: `receipt-${orderId}`,
    amount,
  }),
);

await actae.run({ id: 'order-42', framework: 'example' }, async () => {
  console.log(await createReceipt({ orderId: '42', amount: 2500 }));
});
actae.close();
