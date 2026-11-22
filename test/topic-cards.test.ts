import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type ServerResponse } from "node:http";
import { it } from "node:test";
import type { LarkChannel, NormalizedMessage } from "@larksuiteoapi/node-sdk";
import { Bridge } from "../src/core/bridge.js";
import { adaptMessage, LarkWsConsumer } from "../src/lark/ws-consumer.js";
import { baseConfig } from "./lib/helpers.js";

function incoming(id: string, fields: Partial<NormalizedMessage> = {}) {
  return adaptMessage({
    messageId: id, chatId: "oc_group", chatType: "group", senderId: "ou_user",
    content: id, rawContentType: "text", mentions: [], createTime: 1, ...fields,
  } as NormalizedMessage);
}

function makeBridge(port = 4096, topicChatIds: string[] = []) {
  const bridge = new Bridge({ config: baseConfig({
    replyStyle: "card", opencodePort: port, topicChatIds, requireGroupMention: false,
    manageOpencodeServer: false, idleTimeoutMinutes: 0, messageBatchMs: 60_000,
  }) });
  const sessions = new Map<string, string>();
  Object.assign(bridge["sessions"], {
    getSession: (key: string) => sessions.get(key),
    setSession: (key: string, id: string) => sessions.set(key, id),
  });
  return { bridge, sessions };
}

it("routes the first topic post, follow-ups, regular quotes and DMs before batching", async () => {
  const { bridge } = makeBridge();
  const consumer = new LarkWsConsumer({} as ConstructorParameters<typeof LarkWsConsumer>[0]);
  let lookups = 0;
  consumer["_channel"] = { rawClient: { im: { v1: { chat: { get: async () => {
    lookups++;
    return { data: { chat_mode: "topic" } };
  } } } } } } as unknown as LarkChannel;
  bridge["consumer"] = consumer;
  const handle = (id: string, fields?: Partial<NormalizedMessage>) =>
    bridge["handleInbound"](incoming(id, fields));
  try {
    await Promise.all([handle("om_a"), handle("om_b")]);
    await handle("om_nested", { rootId: "om_a", replyToMessageId: "om_child", threadId: "omt_a" });
    assert.equal(lookups, 1);
    bridge["consumer"] = null;
    await handle("om_quote", { rootId: "om_quoted", replyToMessageId: "om_quoted", threadId: "0" });
    await handle("om_t", { threadId: "omt_mixed" });
    await handle("om_dm", { chatType: "p2p", chatId: "oc_dm", threadId: "omt_ignored" });
    assert.deepEqual([...bridge["pendingQueues"]].map(([key, q]) => [key, q.pendingCount]), [
      ["oc_group/om_a", 2], ["oc_group/om_b", 1], ["oc_group", 1],
      ["oc_group/omt_mixed", 1], ["oc_dm", 1],
    ]);
  } finally {
    for (const queue of bridge["pendingQueues"].values()) queue.discard();
  }
});

it("keeps a rejected card's text fallback in its source topic", async () => {
  const { bridge } = makeBridge(4096, ["oc_group"]);
  bridge["client"].createSession = async () => ({ id: "ses_test" });
  bridge["client"].prompt = async () => ({ text: "done", raw: {} });
  const commands: string[][] = [];
  bridge["sender"]["run"] = async args => {
    commands.push(args);
    if (args.includes("interactive")) throw new Error("card rejected");
    return "{}";
  };
  await bridge["handleInbound"](incoming("om_root"));
  await bridge["pendingQueues"].get("oc_group/om_root")!.drain();
  assert.equal(commands.length, 2);
  for (const args of commands) {
    assert.deepEqual(args.slice(0, 4), ["im", "+messages-reply", "--message-id", "om_root"]);
    assert.ok(args.includes("--reply-in-thread"));
  }
});

it("sends cards directly to ordinary chats and DMs", async () => {
  const { bridge } = makeBridge();
  const commands: string[][] = [];
  bridge["sender"]["run"] = async args => {
    commands.push(args);
    return JSON.stringify({ data: { message_id: "om_card" } });
  };
  // /config uses the normal card delivery path without requiring a model.
  for (const chatType of ["group", "p2p"] as const) {
    await bridge["handleInbound"](incoming("om_request_" + chatType, {
      chatType, chatId: "oc_" + chatType, content: "/config",
    }));
  }
  assert.deepEqual(commands.map(args => args.slice(0, 4)), [
    ["im", "+messages-send", "--chat-id", "oc_group"],
    ["im", "+messages-send", "--chat-id", "oc_p2p"],
  ]);
});

// The local HTTP server speaks OpenCode's session, async prompt and SSE protocols.
// An external test model should stream "checking-" + prompt as a reasoning summary,
// pause for a card refresh, then echo the prompt as visible text before finishing.
// Point TOPIC_TEST_OPENCODE_PORT at its isolated OpenCode server.
it("streams simultaneous topics to their own reply card before each run completes", { timeout: 25_000 }, async () => {
  const streams = new Set<ServerResponse>();
  const emit = (type: string, properties: object) => {
    for (const res of streams) res.write("data: " + JSON.stringify({ type, properties }) + "\n\n");
  };
  let serial = 0;
  const server = createServer(async (req, res) => {
    if (req.url === "/event") {
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.flushHeaders();
      streams.add(res);
      res.on("close", () => streams.delete(res));
      return;
    }
    let body = "";
    for await (const chunk of req) body += chunk;
    if (req.url === "/session") {
      res.end(JSON.stringify({ id: "ses_" + ++serial }));
      return;
    }
    const sid = req.url?.match(/^\/session\/(ses_\w+)\/prompt_async$/)?.[1];
    if (!sid) { res.writeHead(404).end(); return; }
    res.writeHead(204).end();
    const text = JSON.parse(body).parts[0].text;
    emit("session.status", { sessionID: sid, status: { type: "busy" } });
    emit("message.updated", { info: { sessionID: sid, id: sid + "_msg", role: "assistant" } });
    const reasoning = { sessionID: sid, messageID: sid + "_msg", id: sid + "_summary", type: "reasoning", text: "" };
    const summary = "checking-" + text;
    emit("message.part.updated", { part: reasoning });
    emit("message.part.delta", { sessionID: sid, messageID: reasoning.messageID, partID: reasoning.id, field: "text", delta: summary });
    const part = { sessionID: sid, messageID: sid + "_msg", id: sid + "_part", type: "text", text: "" };
    setTimeout(() => {
      emit("message.part.updated", { part: { ...reasoning, text: summary } });
      emit("message.part.updated", { part });
      emit("message.part.delta", { sessionID: sid, messageID: part.messageID, partID: part.id, field: "text", delta: text });
    }, 1800);
    setTimeout(() => {
      emit("message.part.updated", { part: { ...part, text } });
      emit("session.status", { sessionID: sid, status: { type: "idle" } });
    }, 3500);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const externalPort = Number(process.env.TOPIC_TEST_OPENCODE_PORT);
  const { bridge, sessions } = makeBridge(externalPort || (server.address() as { port: number }).port);
  const roots = ["om_alpha", "om_beta"];
  const marker = (root: string) => "reply-" + root.slice(3);
  const created = new Set<string>();
  const updates = new Map<string, Array<{ active: boolean; body: string }>>();
  bridge["sender"]["run"] = async args => {
    const root = args[args.indexOf("--message-id") + 1]!;
    assert.deepEqual(args.slice(0, 4), ["im", "+messages-reply", "--message-id", root]);
    assert.ok(roots.includes(root) && args.includes("--reply-in-thread") && args.includes("interactive"));
    const initial = JSON.parse(args[args.indexOf("--content") + 1]!);
    assert.equal(initial.config.update_multi, true);
    assert.equal(initial.config.streaming_mode, undefined);
    assert.ok(!created.has(root));
    created.add(root);
    return JSON.stringify({ data: { message_id: "card_" + root } });
  };
  bridge["consumer"] = {
    isTopicGroup: async () => false,
    channel: { rawClient: { im: { v1: { message: { patch: async (request: {
      path: { message_id: string }; data: { content: string };
    }) => {
      const id = request.path.message_id;
      const body = request.data.content;
      const card = JSON.parse(body);
      assert.equal(card.config.update_multi, true);
      assert.equal(card.config.streaming_mode, undefined);
      const root = id.slice("card_".length);
      assert.equal(id, "card_" + root);
      const list = updates.get(root) ?? [];
      updates.set(root, list);
      list.push({ active: card.body.elements.some((e: { tag: string }) => e.tag === "button"), body });
      return { code: 0 };
    } } } } } },
  } as unknown as LarkWsConsumer;
  try {
    await Promise.all(roots.map(root => bridge["handleInbound"](incoming(root, {
      threadId: "omt_" + root, content: marker(root),
    }))));
    await Promise.all([...bridge["pendingQueues"].values()].map(q => q.drain()));
    assert.deepEqual(created, new Set(roots));
    assert.equal(new Set(sessions.values()).size, 2);
    for (const root of roots) {
      const list = updates.get(root)!;
      const summary = "checking-" + marker(root);
      const answer = JSON.stringify(marker(root));
      assert.ok(list.some(u => u.active && u.body.includes(summary) && !u.body.includes(answer)), JSON.stringify(list));
      assert.ok(list.some(u => u.active && u.body.includes(answer)), JSON.stringify(list));
      assert.ok(!list.at(-1)!.active && list.at(-1)!.body.includes(marker(root)));
      assert.equal(list.at(-1)!.body.split(summary).length, 2);
      assert.ok(list.every(u => !u.body.includes(marker(roots.find(other => other !== root)!))));
    }
  } finally {
    for (const res of streams) res.end();
    server.closeAllConnections();
    server.close();
  }
});
