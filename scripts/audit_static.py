#!/usr/bin/env python3
"""Static audit for wrong-keyword-argument calls and un-awaited async calls.

Run from the repository root:

    python scripts/audit_static.py [path]

The script parses every Python file under ``path`` (default ``.``) with
:mod:`ast` and reports three classes of likely bugs:

A. Calls that pass a keyword argument the target function does not accept
   (e.g. the historical ``_cleanup_after_success(skip_queued=...)`` bug).
B. Calls to async functions/methods defined in this repository that are
   never awaited (silently dropping a coroutine).
C. Calls to known-async Telegram library methods (python-telegram-bot,
   Pyrogram, Telethon) that are never awaited, when the receiver variable
   looks like a client/bot/message object.  Only unambiguous method names
   are checked so the report stays free of false positives.

The exit code is 0 when no findings are reported and 1 otherwise, so the
script can gate CI on pull requests.  Under GitHub Actions the findings
are emitted as ``::error`` workflow annotations.
"""

import ast
import os
import sys

_EXCLUDE_DIRS = frozenset(
    {
        ".eggs",
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".venv",
        "__pycache__",
        "env",
        "node_modules",
        "venv",
    }
)

# Async Telegram library methods that are unambiguous (no common sync
# collision on other object types).  Names that are also sync methods on
# common objects (start, stop, save, delete, run, open, close, copy,
# download_file, upload_file, ...) are intentionally excluded and must be
# reviewed manually.
_ASYNC_LIB_METHODS = frozenset(
    {
        "answer",
        "answer_callback_query",
        "answer_inline_query",
        "answer_pre_checkout_query",
        "answer_shipping_query",
        "ban_chat_member",
        "copy_message",
        "delete_message",
        "delete_messages",
        "delete_webhook",
        "download_media",
        "download_to_drive",
        "edit_message",
        "edit_message_caption",
        "edit_message_media",
        "edit_message_reply_markup",
        "edit_message_text",
        "export_session_string",
        "forward_message",
        "forward_messages",
        "get_bot_description",
        "get_bot_name",
        "get_bot_short_description",
        "get_chat",
        "get_chat_administrators",
        "get_chat_member",
        "get_chat_members",
        "get_dialogs",
        "get_entity",
        "get_file",
        "get_input_entity",
        "get_me",
        "get_member_count",
        "get_messages",
        "get_my_commands",
        "get_permissions",
        "get_updates",
        "get_user_profile_photos",
        "get_users",
        "get_webhook_info",
        "invite_to_channel",
        "is_user_authorized",
        "iter_messages",
        "join_chat",
        "kick_chat_member",
        "leave_chat",
        "pin_chat_message",
        "reply_animation",
        "reply_audio",
        "reply_contact",
        "reply_copy",
        "reply_dice",
        "reply_document",
        "reply_invoice",
        "reply_location",
        "reply_media_group",
        "reply_photo",
        "reply_poll",
        "reply_sticker",
        "reply_text",
        "reply_venue",
        "reply_video",
        "reply_video_note",
        "reply_voice",
        "resolve_peer",
        "restrict_chat_member",
        "send_animation",
        "send_audio",
        "send_chat_action",
        "send_code",
        "send_contact",
        "send_copy",
        "send_dice",
        "send_document",
        "send_file",
        "send_invoice",
        "send_location",
        "send_media_group",
        "send_message",
        "send_photo",
        "send_poll",
        "send_sticker",
        "send_venue",
        "send_video",
        "send_video_note",
        "send_voice",
        "set_bot_description",
        "set_bot_name",
        "set_bot_short_description",
        "set_my_commands",
        "set_webhook",
        "sign_in",
        "sign_up",
        "unban_chat_member",
        "unpin_chat_message",
    }
)

# Pyrogram ``Storage`` accessors are async (``client.storage.dc_id()`` must
# be awaited).  Only checked when the attribute chain contains a ``storage``
# member so plain reads (``client.session.dc_id``) stay ignored.
_STORAGE_ACCESSORS = frozenset(
    {
        "api_id",
        "auth_key",
        "dc_id",
        "is_bot",
        "is_self",
        "test_mode",
        "user_id",
        "version",
    }
)

# Receiver variable names that indicate a Telegram client / bot / message
# object, making an un-awaited call to a listed method suspicious.
_CLIENT_ROOTS = frozenset(
    {
        "app",
        "application",
        "bot",
        "callback_query",
        "client",
        "context",
        "doc",
        "file",
        "inline_query",
        "message",
        "msg",
        "pyro_client",
        "self",
        "session",
        "storage",
        "telethon_client",
        "update",
        "userbot",
    }
)

# ``start``/``stop`` collide with sync methods on several project classes
# (ProgressTask, CleanupManager, SessionHealthChecker), so only flag them
# when the receiver is clearly a Telegram client.
_CLIENT_START_STOP_ROOTS = frozenset(
    {"client", "pyro_client", "telethon_client", "userbot"}
)

# Functions that consume a coroutine argument, so a nested call is not
# actually un-awaited.
_COROUTINE_CONSUMERS = frozenset(
    {
        "create_task",
        "ensure_future",
        "gather",
        "run",
        "run_until_complete",
        "to_thread",
        "wait",
        "wait_for",
    }
)

_ROOT = "."
_MODULES: dict[str, str] = {}
_DOTTED_BY_PATH: dict[str, str] = {}
_MODULE_DATA: dict[str, tuple] = {}


def _py_files(root: str):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDE_DIRS]
        for name in filenames:
            if name.endswith(".py"):
                yield os.path.join(dirpath, name)


def _index_modules(files: list[str]) -> None:
    """Map dotted module names to normalized file paths."""
    for path in files:
        rel = os.path.relpath(path, _ROOT)
        parts = list(rel.split(os.sep))[:-1]
        stem = os.path.splitext(os.path.basename(rel))[0]
        if stem == "__init__":
            dotted = ".".join(parts)
        else:
            dotted = ".".join(parts + [stem])
        if not dotted:
            continue
        norm = os.path.normpath(path)
        _MODULES[dotted] = norm
        _DOTTED_BY_PATH[norm] = dotted


def _signature(node):
    """Return (is_async, params, kwarg_var) for a function definition."""
    args = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
    params = frozenset(arg.arg for arg in args)
    kwarg_var = node.args.kwarg.arg if node.args.kwarg else None
    return (isinstance(node, ast.AsyncFunctionDef), params, kwarg_var)


def _collect_module(path: str):
    """Parse one module; return (src, tree, funcs, classes, imports)."""
    with open(path, encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src)
    funcs: dict[str, tuple] = {}
    classes: dict[str, dict[str, tuple]] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            funcs[node.name] = _signature(node)
        elif isinstance(node, ast.ClassDef):
            methods = {}
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods[sub.name] = _signature(sub)
            classes[node.name] = methods
    imports: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                imports[alias.asname or parts[0]] = ".".join(parts[:1])
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imports[alias.asname or alias.name] = node.module
    return src, tree, funcs, classes, imports


def _module_for_name(name: str, imports: dict[str, str]) -> str | None:
    """Resolve an imported name to a project module path, if any."""
    dotted = imports.get(name)
    if not dotted:
        return None
    if dotted in _MODULES:
        return _MODULES[dotted]
    for existing, path in _MODULES.items():
        if existing.startswith(dotted + "."):
            return path
    return None


def _enclosing(node, node_type, parents):
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, node_type):
            return cur
        cur = parents.get(cur)
    return None


def _resolve_callable(
    path: str,
    func,
    parents,
    funcs: dict,
    classes: dict,
    imports: dict,
):
    """Resolve a call target to (is_async, params, kwarg_var) or None."""
    if isinstance(func, ast.Name):
        if func.id in funcs:
            return funcs[func.id]
        mod = _module_for_name(func.id, imports)
        if mod:
            return _MODULE_DATA.get(mod, (None, None, {}, {}, {}))[2].get(func.id)
        return None
    if isinstance(func, ast.Attribute):
        value = func.value
        if isinstance(value, ast.Name):
            if value.id in classes:
                return classes[value.id].get(func.attr)
            if value.id == "self":
                cls_node = _enclosing(func, ast.ClassDef, parents)
                if cls_node is not None:
                    return classes.get(cls_node.name, {}).get(func.attr)
            mod = _module_for_name(value.id, imports)
            if mod:
                return _MODULE_DATA.get(mod, (None, None, {}, {}, {}))[2].get(
                    func.attr
                )
            return None
        if isinstance(value, ast.Attribute):
            # Two-level chain: utils.redis_client.get_sync_redis(...)
            root = value.value
            if isinstance(root, ast.Name):
                mod = _module_for_name(root.id, imports)
                if mod:
                    dotted = _DOTTED_BY_PATH.get(mod)
                    sub_mod = _MODULES.get(f"{dotted}.{value.attr}") if dotted else None
                    if sub_mod:
                        return _MODULE_DATA.get(
                            sub_mod, (None, None, {}, {}, {})
                        )[2].get(func.attr)
    return None


def _unawaited(node, parents) -> bool:
    """True when ``node`` (a call) is not awaited or handed to a consumer."""
    child = node
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, ast.Await):
            return False
        if isinstance(cur, ast.AsyncFor) and child is cur.iter:
            return False  # consumed by `async for`
        if isinstance(cur, ast.AsyncWith) and any(
            item.context_expr is child for item in cur.items
        ):
            return False  # consumed by `async with`
        if isinstance(cur, ast.Call):
            fn = cur.func
            if isinstance(fn, ast.Name) and fn.id in _COROUTINE_CONSUMERS:
                return False
            if isinstance(fn, ast.Attribute) and (
                fn.attr in _COROUTINE_CONSUMERS or fn.attr in ("append", "extend")
            ):
                return False
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return True
        child = cur
        cur = parents.get(cur)
    return True


def _root_name(func) -> str | None:
    """Return the root variable name of an attribute chain, or None."""
    cur = func
    while isinstance(cur, ast.Attribute):
        cur = cur.value
    if isinstance(cur, ast.Name):
        return cur.id
    return None


def _chain_attrs(func) -> list[str]:
    attrs = []
    cur = func
    while isinstance(cur, ast.Attribute):
        attrs.append(cur.attr)
        cur = cur.value
    return attrs


def _check_kwargs(path, node, params, kwarg_var, findings, lines) -> None:
    if kwarg_var:
        return
    for kw in node.keywords:
        if kw.arg and kw.arg not in params:
            code = lines[node.lineno - 1].strip() if node.lineno <= len(lines) else ""
            findings.append(
                (
                    path,
                    node.lineno,
                    "wrong-kwarg",
                    f"unexpected keyword {kw.arg}= (expected: {sorted(params)})",
                    code,
                )
            )


def _check_lib_async(path, node, func, parents, findings, lines) -> None:
    if not _unawaited(node, parents):
        return
    attr = func.attr
    root = _root_name(func)
    if attr in ("start", "stop"):
        if root not in _CLIENT_START_STOP_ROOTS:
            return
    elif attr in _ASYNC_LIB_METHODS:
        if root not in _CLIENT_ROOTS:
            return
    elif attr in _STORAGE_ACCESSORS:
        if not ("storage" in _chain_attrs(func) or root == "storage"):
            return
    else:
        return
    code = lines[node.lineno - 1].strip() if node.lineno <= len(lines) else ""
    findings.append(
        (
            path,
            node.lineno,
            "unawaited-lib-async",
            f".{attr}() on `{root}` is async and not awaited",
            code,
        )
    )


def _check_module(path, src, tree, funcs, classes, imports, findings) -> None:
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    lines = src.splitlines()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func

        if isinstance(func, (ast.Name, ast.Attribute)):
            sig = _resolve_callable(path, func, parents, funcs, classes, imports)
            if sig is not None:
                is_async, params, kwarg_var = sig
                _check_kwargs(path, node, params, kwarg_var, findings, lines)
                if is_async and _unawaited(node, parents):
                    code = (
                        lines[node.lineno - 1].strip()
                        if node.lineno <= len(lines)
                        else ""
                    )
                    findings.append(
                        (
                            path,
                            node.lineno,
                            "unawaited-async",
                            f"{func.id if isinstance(func, ast.Name) else func.attr}() "
                            "is async and not awaited",
                            code,
                        )
                    )
        if isinstance(func, ast.Attribute):
            _check_lib_async(path, node, func, parents, findings, lines)


def main(argv: list[str]) -> int:
    global _ROOT, _MODULES, _DOTTED_BY_PATH, _MODULE_DATA
    _ROOT = argv[1] if len(argv) > 1 else "."
    _MODULES = {}
    _DOTTED_BY_PATH = {}
    _MODULE_DATA = {}

    files = sorted(os.path.normpath(p) for p in _py_files(_ROOT))
    _index_modules(files)
    for path in files:
        try:
            _MODULE_DATA[path] = _collect_module(path)
        except Exception:  # a broken file must not crash the audit
            continue

    findings = []
    for path, (src, tree, funcs, classes, imports) in _MODULE_DATA.items():
        _check_module(path, src, tree, funcs, classes, imports, findings)

    findings.sort(key=lambda item: (item[0], item[1]))
    if findings:
        is_actions = os.getenv("GITHUB_ACTIONS") == "true"
        for path, lineno, kind, detail, code in findings:
            if is_actions:
                print(f"::error file={path},line={lineno}::[{kind}] {detail}")
            else:
                print(f"{path}:{lineno}: [{kind}] {detail}  # {code}")
        print(f"\nStatic audit FAILED: {len(findings)} finding(s).")
        return 1
    print(f"Static audit OK: {len(files)} file(s) scanned, no findings.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
