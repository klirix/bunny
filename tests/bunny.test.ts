import { Database } from "bun:sqlite";
import { describe, expect, it } from "bun:test";
import { Bunny } from "../Bunny";

describe("Bunny", () => {
  const db = new Database(":memory:");
  const bunny = new Bunny(db);
  it("acepts messages", async () => {
    const id = bunny.post("stream", { test: "data" });
    expect(typeof id).toBe("string");
    bunny.ack(id);
  });
  it("grabs events for stream synchronously", async () => {
    bunny.post("stream", { test: "data" });
    const event = bunny.grab("stream");
    expect(event instanceof Promise).toBe(false);
    if (event instanceof Promise) return;
    expect(event.eventData.test).toBe("data");
    bunny.ack(event.id);
  });
  it("grabs events for stream asynchronously", async () => {
    const event = bunny.grab("stream");
    const id = bunny.post("stream", { test: "data2" });
    console.log(id);
    expect(event instanceof Promise).toBe(true);
    if (event instanceof Promise)
      expect((await event).eventData.test).toBe("data2");
    bunny.ack(id);
  });
  it("allows for streaming reads of the stream", async () => {
    for (const index of [1, 2, 3]) bunny.post("stream", { index });
    let i = 1;
    for await (const event of bunny.read<{ index: number }>("stream")) {
      expect(event.eventData.index).toBe(i++);
      bunny.ack(event.id);
    }
  });
});
