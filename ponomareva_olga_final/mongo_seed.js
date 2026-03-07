use etl_final;

db.user_sessions.drop();
db.event_logs.drop();
db.support_tickets.drop();
db.user_recommendations.drop();
db.moderation_queue.drop();

function randInt(a, b) { return Math.floor(Math.random() * (b - a + 1)) + a; }
function pick(arr) { return arr[randInt(0, arr.length - 1)]; }
function randDate(daysBack) {
  const now = new Date();
  const t = now.getTime()
    - randInt(0, daysBack) * 24*3600*1000
    - randInt(0, 23) * 3600*1000
    - randInt(0, 59) * 60*1000
    - randInt(0, 59) * 1000;
  return new Date(t);
}

const daysBack = 60;
const users = Array.from({length: 500}, (_, i) => "user_" + (i+1));
const products = Array.from({length: 800}, (_, i) => "prod_" + (i+1));

const pages = ["/home", "/products", "/products/42", "/cart", "/search", "/profile", "/checkout", "/orders"];
const actionList = ["login", "view_product", "search", "add_to_cart", "remove_from_cart", "checkout", "logout"];

const deviceTypes = ["mobile", "desktop", "tablet"];
const osList = ["ios", "android", "macos", "windows"];
const browsers = ["chrome", "safari", "firefox", "edge"];

const eventTypes = ["click", "view", "scroll", "purchase", "add_to_cart", "remove_from_cart"];
const ticketStatuses = ["open", "in_progress", "closed"];
const issueTypes = ["payment", "delivery", "account", "app_bug", "other"];

const moderationStatuses = ["pending", "approved", "rejected"];
const flagList = ["contains_images", "spam_suspected", "toxic_language", "needs_manual_check"];

// 1) user_sessions
let sessions = [];
for (let i = 0; i < 20000; i++) {
  const user_id = pick(users);
  const start = randDate(daysBack);
  const durMin = randInt(2, 90);
  const end = new Date(start.getTime() + durMin*60*1000);
  const pages_visited = Array.from({length: randInt(1, 8)}, () => pick(pages));
  const actions = Array.from({length: randInt(1, 8)}, () => pick(actionList));

  sessions.push({
    session_id: "sess_" + i,
    user_id,
    start_time: start,
    end_time: end,
    pages_visited,
    device: { type: pick(deviceTypes), os: pick(osList), browser: pick(browsers) },
    actions
  });
}
db.user_sessions.insertMany(sessions);

// 2) event_logs
let events = [];
for (let i = 0; i < 50000; i++) {
  const ts = randDate(daysBack);
  const et = pick(eventTypes);
  const user_id = pick(users);
  let details = {};
  if (et === "purchase") details = { user_id, product_id: pick(products), amount: randInt(5, 500) };
  else if (et === "view") details = { user_id, page: pick(pages) };
  else if (et === "click") details = { user_id, element: "btn_" + randInt(1, 50), page: pick(pages) };
  else details = { user_id, page: pick(pages) };

  events.push({ event_id: "evt_" + i, timestamp: ts, event_type: et, details });
}
db.event_logs.insertMany(events);

// 3) support_tickets
let tickets = [];
for (let i = 0; i < 3000; i++) {
  const user_id = pick(users);
  const created = randDate(daysBack);
  const updated = new Date(created.getTime() + randInt(5, 600) * 60*1000);
  const status = pick(ticketStatuses);
  const issue_type = pick(issueTypes);

  const msgCount = randInt(1, 5);
  let messages = [];
  for (let j = 0; j < msgCount; j++) {
    const sender = (j % 2 === 0) ? "user" : "support";
    messages.push({
      sender,
      message: sender === "user" ? ("Проблема: " + issue_type) : "Уточните детали, пожалуйста.",
      timestamp: new Date(created.getTime() + randInt(1, 300) * 60*1000)
    });
  }

  tickets.push({ ticket_id: "ticket_" + i, user_id, status, issue_type, messages, created_at: created, updated_at: updated });
}
db.support_tickets.insertMany(tickets);

// 4) user_recommendations
let recs = [];
for (let i = 0; i < 500; i++) {
  const user_id = users[i];
  const recommended_products = Array.from({length: randInt(3, 12)}, () => pick(products));
  recs.push({ user_id, recommended_products, last_updated: randDate(daysBack) });
}
db.user_recommendations.insertMany(recs);

// 5) moderation_queue
let reviews = [];
for (let i = 0; i < 4000; i++) {
  const user_id = pick(users);
  const product_id = pick(products);
  const rating = randInt(1, 5);
  const flags = Array.from({length: randInt(0, 3)}, () => pick(flagList));

  reviews.push({
    review_id: "rev_" + i,
    user_id,
    product_id,
    review_text: "Товар " + (rating >= 4 ? "понравился" : "не понравился") + ", оценка " + rating,
    rating,
    moderation_status: pick(moderationStatuses),
    flags,
    submitted_at: randDate(daysBack)
  });
}
db.moderation_queue.insertMany(reviews);

print("OK:",
  "sessions=", db.user_sessions.countDocuments(),
  "events=", db.event_logs.countDocuments(),
  "tickets=", db.support_tickets.countDocuments(),
  "recs=", db.user_recommendations.countDocuments(),
  "reviews=", db.moderation_queue.countDocuments()
);
