let sequence = 0;
export const unique = (prefix: string) => `${prefix}.${String(Date.now())}.${String(++sequence)}`;
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function waitFor(probe: () => Promise<boolean> | boolean, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await probe())) {
    if (Date.now() > deadline) throw new Error('Timed out waiting');
    await sleep(50);
  }
}
