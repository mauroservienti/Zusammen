import { execFileSync, spawn, spawnSync } from 'node:child_process';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { waitFor } from './environment.js';

const project = fileURLToPath(new URL('../../../dotnet/compat/Zusammen.Compat.Endpoint', import.meta.url));
const assembly = `${project}/bin/Release/net10.0/Zusammen.Compat.Endpoint.dll`;

export const dotnetAvailable = spawnSync('dotnet', ['--version']).status === 0;

/** Builds the compat endpoint; once per test run. */
export function buildEndpoint(): void {
  execFileSync('dotnet', ['build', project, '-c', 'Release', '--nologo', '-v', 'quiet'], { stdio: 'inherit' });
}

export interface EndpointReport {
  event: 'ready' | 'handled' | 'failed';
  type?: string;
  MessageId?: string;
  messageId?: string;
  body?: Record<string, unknown>;
  headers?: Record<string, string>;
  error?: string;
}

/** A running NServiceBus endpoint (NServiceBus 10, RabbitMQ transport) reporting what it handles. */
export async function startEndpoint(options: {
  amqpUrl: string;
  managementUrl: string;
  name: string;
  topology: 'conventional' | 'direct';
  mongoConnectionString: string;
  zusammen?: boolean;
  outbox?: boolean;
}) {
  const args = [assembly, '--amqp', options.amqpUrl, '--management', options.managementUrl];
  args.push('--mongo', options.mongoConnectionString);
  args.push('--endpoint', options.name, '--topology', options.topology);
  if (options.zusammen === true) args.push('--zusammen');
  if (options.outbox === true) args.push('--outbox');

  const child = spawn('dotnet', args, { stdio: ['pipe', 'pipe', 'inherit'] });
  const reports: EndpointReport[] = [];
  createInterface({ input: child.stdout }).on('line', (line) => {
    if (line.startsWith('ZUSAMMEN ')) reports.push(JSON.parse(line.slice('ZUSAMMEN '.length)) as EndpointReport);
  });
  let exited = false;
  child.once('exit', () => (exited = true));

  await waitFor(() => {
    if (exited) throw new Error(`Endpoint ${options.name} exited during startup`);
    return reports.some((report) => report.event === 'ready');
  }, 90_000);

  return {
    reports,
    handled: () => reports.filter((report) => report.event === 'handled'),
    failed: () => reports.filter((report) => report.event === 'failed'),
    async stop() {
      child.stdin.end();
      await new Promise((resolve) => child.once('exit', resolve));
    },
  };
}
