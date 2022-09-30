import { Bunny, GrabTimedoutError } from "./Bunny";

// const db = Database.open("bunnybase.db");

const bunny = new Bunny();

Bun.serve({
  async fetch(req) {
    const start = Date.now();
    const url = new URL(req.url);
    let res: Response = Response.error();
    switch (true) {
      case url.pathname === "/post" && req.method === "POST":
        const postdata = (await req.json()) as any;
        const id = bunny.post(postdata.streamName, postdata.data);
        res = Response.json({ id });
        break;
      case url.pathname === "/show" && req.method === "GET":
        res = Response.json({
          events: bunny.show(),
          timeouts: Object.fromEntries(bunny.eventTimeouts),
        });
        break;
      case url.pathname === "/grab" && req.method === "GET":
        const streamName = url.searchParams.get("streamName");
        try {
          res = Response.json(await bunny.grab(streamName, 10000, 5000));
        } catch (error) {
          if (error instanceof GrabTimedoutError)
            res = Response.json(
              { error: "Try again later", errorCode: "later" },
              { status: 408 }
            );
          res = Response.error();
        }
        break;
    }

    Bun.write(Bun.stdout, `Took ${Date.now() - start} to process req\n`);
    return res;
  },
  port: 3005,
});
