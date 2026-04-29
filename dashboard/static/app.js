'use strict';

// Read API base from meta tag (e.g. "" for same-origin, "/api" for proxied).
const API_BASE = (document.getElementById('api-base') || {}).content || '';

// Read auth config injected by the server. Recognised keys:
//   auth-type        — "cookie" | "bearer" | "none" (default: "none")
//   auth-credentials — fetch credentials mode for cookie auth (default: "same-origin")
//   auth-header      — header name for bearer auth (default: "Authorization")
const AUTH_CONFIG = (() => {
  try {
    return JSON.parse((document.getElementById('auth-config') || {}).content || '{}');
  } catch (_) { return {}; }
})();
const AUTH_TYPE = AUTH_CONFIG['auth-type'] || 'none';

// Bearer token — stored in sessionStorage so it survives page refreshes
// within the same tab but is cleared when the tab closes.
const BEARER_TOKEN_KEY = 'dashboard:bearer-token';
let bearerToken = sessionStorage.getItem(BEARER_TOKEN_KEY) || '';

// ── State ──────────────────────────────────────────────────────────────────
let currentNamespace = '';
let currentCursor    = '';
let totalLoaded      = 0;
const recentNS       = []; // last-used namespaces (up to 5)

// ── Auth UI ────────────────────────────────────────────────────────────────
const authSection   = document.getElementById('auth-section');
const authInput     = document.getElementById('auth-token-input');
const authSaveBtn   = document.getElementById('auth-token-save');

if (AUTH_TYPE === 'bearer') {
  authSection.style.display = 'block';
  authInput.value = bearerToken;
}

authSaveBtn.addEventListener('click', () => {
  bearerToken = authInput.value.trim();
  sessionStorage.setItem(BEARER_TOKEN_KEY, bearerToken);
  showToast('Token saved.', 'success');
});

// ── DOM refs ───────────────────────────────────────────────────────────────
const nsInput       = document.getElementById('ns-input');
const loadBtn       = document.getElementById('load-btn');
const tableBody     = document.getElementById('keys-table-body');
const tableWrapper  = document.getElementById('table-wrapper');
const loadMoreBtn   = document.getElementById('load-more');
const createSection = document.getElementById('create-section');
const createForm    = document.getElementById('create-form');
const formKey       = document.getElementById('form-key');
const formCodec     = document.getElementById('form-codec');
const formValue     = document.getElementById('form-value');
const nsHeader      = document.getElementById('ns-header');
const nsTitle       = document.getElementById('ns-title');
const keyCount      = document.getElementById('key-count');
const emptyState    = document.getElementById('empty-state');
const recentSection = document.getElementById('recent-section');
const recentList    = document.getElementById('recent-list');

// Modal refs
const editModal   = document.getElementById('edit-modal');
const editNs      = document.getElementById('edit-ns');
const editKeyEl   = document.getElementById('edit-key');
const editCodec   = document.getElementById('edit-codec');
const editValue   = document.getElementById('edit-value');
const editSave    = document.getElementById('edit-save');
const editCancel  = document.getElementById('edit-cancel');

// Toast ref
const toastEl = document.getElementById('toast');
let toastTimer = null;

// ── Toast ──────────────────────────────────────────────────────────────────
function showToast(msg, type) {
  toastEl.textContent = msg;
  toastEl.className   = 'toast toast-' + (type || 'info');
  toastEl.style.display = 'block';
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toastEl.style.display = 'none'; }, 3500);
}

// ── API helpers ────────────────────────────────────────────────────────────

/**
 * Encode a plain-text string to base64 so it can be sent as bytes in the
 * protobuf JSON wire format that grpc-gateway expects for `bytes` fields.
 */
function encodeValue(text) {
  // btoa works on binary strings; encode to UTF-8 bytes first.
  return btoa(unescape(encodeURIComponent(text)));
}

/**
 * Decode a base64-encoded bytes field from the grpc-gateway JSON response.
 * Returns the raw string. Returns '' if input is falsy.
 */
function decodeValue(b64) {
  if (!b64) return '';
  try {
    return decodeURIComponent(escape(atob(b64)));
  } catch (_) {
    // Fall back to raw base64 if decode fails (binary data).
    return b64;
  }
}

async function apiFetch(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' },
  };

  if (AUTH_TYPE === 'cookie') {
    // Let the browser forward the session/JWT cookie automatically.
    opts.credentials = AUTH_CONFIG['auth-credentials'] || 'include';
  } else if (AUTH_TYPE === 'bearer') {
    if (!bearerToken) {
      throw new Error('No auth token set — enter your token in the sidebar.');
    }
    const header = AUTH_CONFIG['auth-header'] || 'Authorization';
    opts.headers[header] = 'Bearer ' + bearerToken;
  }

  if (body !== undefined) {
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(API_BASE + path, opts);
  if (!resp.ok) {
    if (resp.status === 401 && AUTH_TYPE === 'bearer') {
      throw new Error('Authentication failed — check your token in the sidebar.');
    }
    const text = await resp.text().catch(() => resp.statusText);
    throw new Error(text || resp.statusText);
  }
  // 204 No Content
  if (resp.status === 204) return null;
  return resp.json();
}

// ── Key loading ────────────────────────────────────────────────────────────

async function loadKeys(namespace, cursor) {
  if (!namespace) {
    showToast('Please enter a namespace.', 'error');
    return;
  }

  const isFirstPage = !cursor;

  let url = '/v1/namespaces/' + encodeURIComponent(namespace) + '/keys?limit=50';
  if (cursor) url += '&cursor=' + encodeURIComponent(cursor);

  let data;
  try {
    data = await apiFetch('GET', url);
  } catch (err) {
    showToast('Failed to load keys: ' + err.message, 'error');
    return;
  }

  const entries = data.entries || [];

  if (isFirstPage) {
    tableBody.innerHTML = '';
    totalLoaded = 0;
    addToRecent(namespace);
  }

  entries.forEach(entry => {
    tableBody.appendChild(renderRow(namespace, entry));
    totalLoaded++;
  });

  // grpc-gateway default marshaler emits camelCase (protojson). We accept
  // snake_case as a fallback in case a caller sets UseProtoNames=true.
  currentCursor = data.nextCursor || data.next_cursor || '';
  loadMoreBtn.style.display = currentCursor ? 'inline-block' : 'none';

  // Show / hide UI sections.
  const hasRows = tableBody.rows.length > 0;
  emptyState.style.display  = hasRows ? 'none' : 'flex';
  tableWrapper.style.display = hasRows ? 'block' : 'none';
  createSection.style.display = 'block';
  nsHeader.style.display = 'flex';
  nsTitle.textContent    = namespace;
  keyCount.textContent   = totalLoaded + ' key' + (totalLoaded !== 1 ? 's' : '');

  if (isFirstPage && entries.length === 0) {
    emptyState.style.display  = 'flex';
    tableWrapper.style.display = 'none';
  }
}

// ── Row rendering ──────────────────────────────────────────────────────────

function renderRow(namespace, entry) {
  const tr = document.createElement('tr');

  const rawValue   = decodeValue(entry.value);
  // grpc-gateway default marshaler emits camelCase; accept snake_case as a fallback.
  const updatedTs  = entry.updatedAt || entry.updated_at;
  const updatedAt  = updatedTs ? formatDate(updatedTs) : '';
  const codec      = entry.codec || 'raw';
  const version    = entry.version != null ? entry.version : '';

  tr.innerHTML = `
    <td class="cell-key">${escHtml(entry.key)}</td>
    <td class="cell-value" title="${escAttr(rawValue)}" data-key="${escAttr(entry.key)}">${escHtml(truncate(rawValue, 80))}</td>
    <td class="cell-codec">${escHtml(codec)}</td>
    <td class="cell-version">${escHtml(String(version))}</td>
    <td class="cell-updated">${escHtml(updatedAt)}</td>
    <td class="cell-actions">
      <button class="btn btn-secondary btn-sm edit-btn">Edit</button>
      <button class="btn btn-danger btn-sm delete-btn">Delete</button>
    </td>
  `;

  tr.querySelector('.cell-value').addEventListener('click', () => {
    openEdit(namespace, entry.key, rawValue, codec);
  });

  tr.querySelector('.edit-btn').addEventListener('click', () => {
    openEdit(namespace, entry.key, rawValue, codec);
  });

  tr.querySelector('.delete-btn').addEventListener('click', () => {
    deleteKey(namespace, entry.key, tr);
  });

  return tr;
}

// ── Create form ────────────────────────────────────────────────────────────

createForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  const key   = formKey.value.trim();
  const codec = formCodec.value;
  const value = formValue.value;

  if (!key) { showToast('Key is required.', 'error'); return; }
  if (!currentNamespace) { showToast('Load a namespace first.', 'error'); return; }

  try {
    await apiFetch('POST', '/v1/namespaces/' + encodeURIComponent(currentNamespace) + '/keys/' + encodeURIComponent(key), {
      value: encodeValue(value),
      codec: codec,
    });
    showToast('Key saved.', 'success');
    formKey.value   = '';
    formValue.value = '';
    // Reload to reflect the new/updated entry.
    await loadKeys(currentNamespace);
  } catch (err) {
    showToast('Save failed: ' + err.message, 'error');
  }
});

// ── Edit modal ─────────────────────────────────────────────────────────────

function openEdit(namespace, key, value, codec) {
  editNs.value     = namespace;
  editKeyEl.value  = key;
  editValue.value  = value;
  // Select matching codec option.
  const opt = editCodec.querySelector('option[value="' + codec + '"]');
  if (opt) opt.selected = true;
  editModal.style.display = 'flex';
  editValue.focus();
}

function closeEditModal() {
  editModal.style.display = 'none';
}

editCancel.addEventListener('click', closeEditModal);

editModal.addEventListener('click', (e) => {
  if (e.target === editModal) closeEditModal();
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && editModal.style.display !== 'none') closeEditModal();
});

editSave.addEventListener('click', async () => {
  const ns    = editNs.value;
  const key   = editKeyEl.value;
  const value = editValue.value;
  const codec = editCodec.value;

  try {
    await apiFetch('POST', '/v1/namespaces/' + encodeURIComponent(ns) + '/keys/' + encodeURIComponent(key), {
      value: encodeValue(value),
      codec: codec,
    });
    showToast('Key updated.', 'success');
    closeEditModal();
    await loadKeys(ns);
  } catch (err) {
    showToast('Update failed: ' + err.message, 'error');
  }
});

// ── Delete ─────────────────────────────────────────────────────────────────

async function deleteKey(namespace, key, rowEl) {
  if (!confirm('Delete key "' + key + '" from namespace "' + namespace + '"?')) return;

  try {
    await apiFetch('DELETE', '/v1/namespaces/' + encodeURIComponent(namespace) + '/keys/' + encodeURIComponent(key));
    rowEl.remove();
    totalLoaded = Math.max(0, totalLoaded - 1);
    keyCount.textContent = totalLoaded + ' key' + (totalLoaded !== 1 ? 's' : '');

    if (tableBody.rows.length === 0) {
      tableWrapper.style.display = 'none';
      emptyState.style.display   = 'flex';
    }
    showToast('Key deleted.', 'success');
  } catch (err) {
    showToast('Delete failed: ' + err.message, 'error');
  }
}

// ── Load more ──────────────────────────────────────────────────────────────

loadMoreBtn.addEventListener('click', () => {
  loadKeys(currentNamespace, currentCursor);
});

// ── Recent namespaces ──────────────────────────────────────────────────────

function addToRecent(ns) {
  const idx = recentNS.indexOf(ns);
  if (idx !== -1) recentNS.splice(idx, 1);
  recentNS.unshift(ns);
  if (recentNS.length > 5) recentNS.pop();
  renderRecent();
}

function renderRecent() {
  recentList.innerHTML = '';
  recentNS.forEach(ns => {
    const li  = document.createElement('li');
    const btn = document.createElement('button');
    btn.type        = 'button';
    btn.textContent = ns;
    btn.addEventListener('click', () => {
      nsInput.value      = ns;
      currentNamespace   = ns;
      loadKeys(ns);
    });
    li.appendChild(btn);
    recentList.appendChild(li);
  });
  recentSection.style.display = recentNS.length > 0 ? 'block' : 'none';
}

// ── Init ───────────────────────────────────────────────────────────────────

loadBtn.addEventListener('click', () => {
  const ns = nsInput.value.trim();
  if (!ns) { showToast('Please enter a namespace.', 'error'); return; }
  currentNamespace = ns;
  currentCursor    = '';
  loadKeys(currentNamespace);
});

nsInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') loadBtn.click();
});

// ── Utilities ──────────────────────────────────────────────────────────────

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function escAttr(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;');
}

function truncate(str, max) {
  if (str.length <= max) return str;
  return str.slice(0, max) + '…';
}

function formatDate(ts) {
  if (!ts) return '';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return ts;
    return d.toLocaleString();
  } catch (_) {
    return ts;
  }
}
