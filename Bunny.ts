import { Database, Statement } from "bun:sqlite";

interface IEvent<T = any> {
  id: string;
  streamName: string;
  eventData: T;
  at: string;
}
interface IBunny {
  post(streamName: string, data: any): string;
  grab<T = any>(streamName: string, timeout: number): MaybePromise<IEvent<T>>;
  ack(id: string): void;
  nack(id: string): void;
  read<T>(name: string): Generator<IEvent<T>, void, unknown>;
}
type MaybePromise<T> = Promise<T> | T;
export class GrabTimedoutError extends Error {}
export class Bunny implements IBunny {
  private postStmnt: Statement;
  private grabStatement: Statement;
  private setProcessing: Statement;
  private dropEventStatement: Statement;
  private getEvent: Statement;
  constructor(private db: Database = new Database()) {
    db.exec(`
      create table if not exists events (
        id text primary key,
        streamName text not null,
        eventData text not null,
        at text not null,
        processing integer
      )
    `);
    this.showStream = db.prepare(
      "select id, eventData, at from events where streamName = ? order by at asc"
    );
    this.postStmnt = db.prepare("insert into events values (?, ?, ?, ?, ?)");

    this.grabStatement = db.prepare(`
      select id, eventData, at 
      from events 
      where streamName = ? and processing != 1
      order by at
      limit 1
    `);

    this.setProcessing = db.prepare(
      `update events set processing = ? where id = ?`
    );
    this.dropEventStatement = db.prepare(`delete from events where id = ?`);
    this.getEvent = db.prepare(
      `select id, eventData, at, processing from events  where id = ? limit 1`
    );
    // Bun.write(Bun.stdout, "Bunny initialized\n");
  }

  private showStream;

  post(streamName: string, data: any): string {
    const newId = crypto.randomUUID();
    const firstWaiter = this.waiters[streamName]?.pop();
    const jsonData = JSON.stringify(data);
    this.postStmnt.run(
      newId,
      streamName,
      jsonData,
      new Date().toISOString(),
      false
    );
    if (firstWaiter) {
      firstWaiter(newId);
      this.freeWaiters(streamName);
    }
    return newId;
  }

  waiters: Record<string, Array<(id: string) => void>> = {};

  private freeWaiters(streamName: string) {
    if (this.waiters[streamName].length === 0) delete this.waiters[streamName];
  }

  private eventFromValues<T>(
    [id, eventData, at]: (string | number | bigint | boolean | Uint8Array)[],
    streamName: string
  ): IEvent<T> {
    return {
      id: id as string,
      streamName,
      at: at as string,
      eventData: JSON.parse(eventData as string),
    };
  }

  eventTimeouts = new Map<string, number>();

  grab<T = any>(
    streamName: string,
    timeout?: number,
    ackTimeout?: number
  ): MaybePromise<IEvent<T>> {
    // Bun.write(Bun.stdout, `Trying to grab event from ${streamName}\n`);
    const [event] = this.grabStatement.values(streamName);
    if (event) {
      const parsedEvent = this.eventFromValues<T>(event, streamName);
      Bun.write(
        Bun.stdout,
        `Event ${parsedEvent.id} found for ${streamName}\n`
      );
      this.setProcessing.run(true, parsedEvent.id);
      Bun.write(
        Bun.stdout,
        `Will timeout ${parsedEvent.id} after ${ackTimeout}\n`
      );
      if (ackTimeout !== undefined)
        this.addEventTimeout(parsedEvent.id, streamName, ackTimeout);
      return parsedEvent;
    }
    return new Promise((res, rej) => {
      const waitList = this.waiters[streamName];
      const onPushed = (id: string) => {
        const [event] = this.getEvent.values(id);
        this.setProcessing.run(true, id);
        res(this.eventFromValues(event, streamName));
        if (ackTimeout !== undefined)
          this.addEventTimeout<T>(id, streamName, ackTimeout);
      };
      if (waitList) {
        waitList.push();
      } else {
        this.waiters[streamName] = [onPushed];
      }
      if (timeout)
        setTimeout(() => {
          this.waiters[streamName] = this.waiters[streamName]?.filter(
            (x) => x !== onPushed
          );
          this.freeWaiters(streamName);
          rej(new GrabTimedoutError());
        }, timeout);
    });
  }

  private addEventTimeout<T = any>(
    id: string,
    streamName: string,
    ackTimeout: number
  ) {
    const started = Date.now();
    this.eventTimeouts.set(
      id,
      setTimeout(() => {
        console.log(Date.now() - started, "vs", ackTimeout);
        Bun.write(
          Bun.stdout,
          `Event ${id} for ${streamName} took to long to process, requeueing\n`
        );
        this.nack(id, streamName);
      }, ackTimeout)
    );
  }

  ack(id: string) {
    try {
      this.dropEventStatement.run(id);
      this.eventTimeouts.delete(id);
    } catch (error) {
      console.error(error);
    }
  }
  nack(id: string, streamName?: string) {
    this.setProcessing.run(false, id);
    if (!streamName) return;
    const firstWaiter = this.waiters[streamName]?.pop();
    this.eventTimeouts.delete(id);
    if (!firstWaiter) return;
    firstWaiter(id);
    this.freeWaiters(streamName);
  }

  show() {
    return this.db.query("select * from events").all();
  }

  *read<T>(streamName: string) {
    for (const event of this.showStream.values(streamName))
      yield this.eventFromValues<T>(event, streamName);
  }
}
