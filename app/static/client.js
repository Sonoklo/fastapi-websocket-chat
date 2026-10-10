/*
 * Клиентская часть чата.
 *
 * Браузерный WebSocket API крошечный — всего один класс и четыре события:
 *   onopen    — соединение установлено
 *   onmessage — пришли данные
 *   onclose   — соединение закрылось (по любой причине)
 *   onerror   — произошла ошибка
 *
 * Вся сложность реального клиента не в API, а в двух вещах:
 *   1) переподключение при обрыве связи,
 *   2) состояние интерфейса, пока соединения нет.
 */

// ---------------------------------------------------------------------
// Состояние
// ---------------------------------------------------------------------

let socket = null;          // текущий объект WebSocket
let reconnectAttempts = 0;  // счётчик попыток для экспоненциальной задержки
let manualClose = false;    // мы закрыли сами? тогда переподключаться не надо
let typingTimer = null;     // таймер «пользователь перестал печатать»

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------------
// Подключение
// ---------------------------------------------------------------------

function connect(user, room) {
  // Схема выбирается по схеме страницы: на HTTPS обязателен WSS, иначе
  // браузер заблокирует «смешанный контент» и соединение просто не откроется.
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";

  // encodeURIComponent обязателен: имя может содержать пробелы, & или #,
  // которые сломают строку запроса.
  const url = `${scheme}//${location.host}/ws/${encodeURIComponent(room)}`
            + `?user=${encodeURIComponent(user)}`;

  manualClose = false;
  socket = new WebSocket(url);

  // --- Соединение открыто ---
  socket.onopen = () => {
    reconnectAttempts = 0;   // сбрасываем задержку после успеха
    setStatus("online", "подключено");
    $("composer").hidden = false;
    $("login").hidden = true;
  };

  // --- Пришло сообщение ---
  socket.onmessage = (event) => {
    // event.data — ВСЕГДА строка (или Blob для бинарных фреймов).
    // Автоматического разбора JSON здесь нет, парсим сами.
    let data;
    try {
      data = JSON.parse(event.data);
    } catch {
      console.warn("Пришёл не-JSON:", event.data);
      return;
    }
    handleServerEvent(data);
  };

  // --- Соединение закрылось ---
  socket.onclose = (event) => {
    $("composer").hidden = true;

    // Код 1008 мы шлём с сервера при отказе (например, пустое имя).
    // Переподключаться в этом случае бессмысленно — результат будет тот же.
    if (manualClose || event.code === 1008) {
      setStatus("offline", "отключено");
      $("login").hidden = false;
      return;
    }

    // Экспоненциальная задержка: 1с, 2с, 4с, 8с… но не более 15с.
    //
    // Почему не переподключаться сразу в цикле? Если упал сервер, то сотня
    // клиентов, долбящихся каждые 100 мс, не дадут ему подняться. Растущая
    // пауза распределяет нагрузку во времени.
    reconnectAttempts += 1;
    const delay = Math.min(1000 * 2 ** (reconnectAttempts - 1), 15000);
    setStatus("connecting", `переподключение через ${delay / 1000}с…`);
    setTimeout(() => connect(user, room), delay);
  };

  // --- Ошибка ---
  socket.onerror = () => {
    // Важная деталь: onerror в браузере НЕ сообщает причину — из соображений
    // безопасности там всегда пустое событие. Реальная информация приходит в
    // onclose (поле code), который всегда срабатывает следом за onerror.
    // Поэтому логику восстановления пишут в onclose, а не здесь.
    console.warn("Ошибка WebSocket (детали будут в onclose)");
  };
}

// ---------------------------------------------------------------------
// Обработка событий сервера
// ---------------------------------------------------------------------

function handleServerEvent(data) {
  // Тот самый дискриминатор `type`, о котором договорились в schemas.py.
  switch (data.type) {
    case "message":
      addMessage(data.user, data.text, data.timestamp);
      break;
    case "system":
      addSystem(data.text);
      break;
    case "users":
      renderUsers(data.users);
      break;
    case "typing":
      renderTyping(data.user, data.is_typing);
      break;
    case "error":
      addSystem(`Ошибка: ${data.detail}`, true);
      break;
    default:
      console.warn("Неизвестный тип события:", data.type);
  }
}

// ---------------------------------------------------------------------
// Отправка
// ---------------------------------------------------------------------

function send(payload) {
  // Проверка readyState обязательна: отправка в закрывающийся сокет бросает
  // исключение. Состояния: 0 CONNECTING, 1 OPEN, 2 CLOSING, 3 CLOSED.
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify(payload));
  return true;
}

function sendMessage() {
  const input = $("text");
  const text = input.value.trim();
  if (!text) return;

  if (send({ type: "message", text })) {
    input.value = "";
    // Своё сообщение в DOM НЕ добавляем — ждём эха от сервера.
    // Так на экране только то, что реально доставлено (см. main.py).
    send({ type: "typing", is_typing: false });
  }
}

function notifyTyping() {
  send({ type: "typing", is_typing: true });

  // Debounce: событие «перестал печатать» шлём один раз через 1.5 с тишины,
  // а не на каждое нажатие клавиши. Иначе при быстрой печати мы завалили бы
  // сервер сотнями бесполезных фреймов.
  clearTimeout(typingTimer);
  typingTimer = setTimeout(() => send({ type: "typing", is_typing: false }), 1500);
}

// ---------------------------------------------------------------------
// Рендеринг
// ---------------------------------------------------------------------

function addMessage(user, text, timestamp) {
  const el = document.createElement("div");
  el.className = "msg";

  const time = new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });

  const head = document.createElement("div");
  head.className = "msg-head";
  head.textContent = `${user} · ${time}`;

  const body = document.createElement("div");
  body.className = "msg-body";
  // textContent, а НЕ innerHTML. Это защита от XSS: если написать в чат
  // <img src=x onerror=alert(1)>, при innerHTML скрипт выполнился бы у всех
  // участников. textContent вставляет текст как текст.
  body.textContent = text;

  el.append(head, body);
  appendToLog(el);
}

function addSystem(text, isError = false) {
  const el = document.createElement("div");
  el.className = isError ? "system error" : "system";
  el.textContent = text;
  appendToLog(el);
}

function appendToLog(el) {
  const log = $("log");
  // Автопрокрутка — только если пользователь уже внизу. Иначе мы бы дёргали
  // экран человеку, который листает историю вверх.
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 60;
  log.appendChild(el);
  if (atBottom) log.scrollTop = log.scrollHeight;
}

function renderUsers(users) {
  $("users").textContent = users.join(", ") || "—";
  $("user-count").textContent = users.length;
}

const typingUsers = new Set();

function renderTyping(user, isTyping) {
  // Set, а не булев флаг: печатать могут одновременно несколько человек.
  if (isTyping) typingUsers.add(user);
  else typingUsers.delete(user);

  const list = [...typingUsers];
  $("typing").textContent = list.length
    ? `${list.join(", ")} печата${list.length > 1 ? "ют" : "ет"}…`
    : "";
}

function setStatus(cls, text) {
  const el = $("status");
  el.className = `status ${cls}`;
  el.textContent = text;
}

// ---------------------------------------------------------------------
// Инициализация
// ---------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  $("login-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const user = $("username").value.trim();
    const room = $("room").value.trim() || "general";
    if (!user) return;
    $("room-name").textContent = room;
    setStatus("connecting", "подключение…");
    connect(user, room);
  });

  $("composer").addEventListener("submit", (e) => {
    e.preventDefault();
    sendMessage();
  });

  $("text").addEventListener("input", notifyTyping);

  // Закрываем соединение явно при уходе со страницы. Без этого сервер узнает
  // об отключении только по таймауту — до полутора минут, и всё это время
  // остальные видят «призрака» в списке участников.
  window.addEventListener("beforeunload", () => {
    manualClose = true;
    socket?.close(1000, "Пользователь ушёл");
  });
});
