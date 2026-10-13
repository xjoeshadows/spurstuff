#!/usr/bin/env python3
import os
import datetime
import sys
import json
import re
import multiprocessing # For true CPU parallelism
import time
import requests # For downloading feeds
import gzip # For decompressing feeds
import shutil # For moving files safely
import atexit # For restoring the terminal cursor on exit
import contextlib # For the keyboard input context manager
import itertools # For grouping wordmark colour runs

# --- Dependency Checks ---
try:
    import maxminddb
except ImportError:
    print("[-] Error: 'maxminddb' library not found.", file=sys.stderr)
    print("[-] Please install it using: pip install maxminddb", file=sys.stderr)
    sys.exit(1)
import ipaddress

# --- Configuration ---
# API Token for downloading feeds (if chosen by user)
API_TOKEN = os.environ.get('TOKEN') 

# --- Terminal UI ---
ANSI_RE = re.compile(r'\x1b\[[0-9;?]*[A-Za-z]')

try:
    import termios
    import tty
    import select as _select
except ImportError:  # Windows
    termios = tty = _select = None
try:
    import msvcrt
except ImportError:
    msvcrt = None


def _vlen(s):
    """Visible length of a string (ignores ANSI escape codes)."""
    return len(ANSI_RE.sub('', s))

def _clip(s, limit):
    """Truncates a string to `limit` visible characters, keeping ANSI codes intact."""
    if _vlen(s) <= limit:
        return s
    out, count, i = [], 0, 0
    while i < len(s) and count < limit - 1:
        m = ANSI_RE.match(s, i)
        if m:
            out.append(m.group(0))
            i = m.end()
            continue
        out.append(s[i])
        count += 1
        i += 1
    return ''.join(out) + '…' + ('\x1b[0m' if '\x1b' in s else '')

def _fmt_bytes(n):
    if n < 1024:
        return f"{int(n)} B"
    for unit in ('KB', 'MB', 'GB'):
        n /= 1024
        if n < 1024 or unit == 'GB':
            return f"{n:.1f} {unit}"

def _fmt_secs(s):
    s = int(s)
    if s >= 3600:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    if s >= 60:
        return f"{s // 60}m {s % 60}s"
    return f"{s}s"


class UI:
    """Step-rail terminal UI. Falls back to plain input()/print() when not attached to a TTY."""
    STEPS = ["Feed", "Date", "Download", "Filter"]
    WORDMARK = [
        "███████╗██████╗ ██╗   ██╗██████╗ ",
        "██╔════╝██╔══██╗██║   ██║██╔══██╗",
        "███████╗██████╔╝██║   ██║██████╔╝",
        "╚════██║██╔═══╝ ██║   ██║██╔══██╗",
        "███████║██║     ╚██████╔╝██║  ██║",
        "╚══════╝╚═╝      ╚═════╝ ╚═╝  ╚═╝",
    ]
    SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def __init__(self):
        tty_out = sys.stdout.isatty() and os.environ.get('TERM') != 'dumb'
        self.interactive = bool(tty_out and sys.stdin.isatty() and not os.environ.get('SPUR_PLAIN'))
        self.color = bool(tty_out and 'NO_COLOR' not in os.environ)
        self.done = 0
        self.idle = False
        self.answers = []
        self.notes = []
        self.lines_above = 0
        self.rail_row = 6
        self.rail_mode = 'compact'
        self.launch = True  # the first screen drawn gets the poster header

    def enable(self):
        if os.name == 'nt':
            os.system('')  # turns on ANSI escape handling in Windows consoles
        if self.interactive:
            try:
                sys.stdout.reconfigure(encoding='utf-8')
            except Exception:
                pass
            atexit.register(self._restore_cursor)

    @staticmethod
    def _restore_cursor():
        try:
            sys.stdout.write('\x1b[?25h')
            sys.stdout.flush()
        except Exception:
            pass

    # --- Styling ---
    def _sgr(self, code, text):
        return f"\x1b[{code}m{text}\x1b[0m" if self.color else text

    def acc(self, t): return self._sgr('38;5;190', t)
    def dim(self, t): return self._sgr('38;5;245', t)
    def bold(self, t): return self._sgr('1', t)
    def rule(self, t): return self._sgr('38;5;240', t)
    def track(self, t): return self._sgr('38;5;238', t)
    def red(self, t): return self._sgr('38;5;203', t)
    def amber(self, t): return self._sgr('38;5;179', t)

    @staticmethod
    def tilde(path):
        home = os.path.expanduser('~')
        return '~' + path[len(home):] if path.startswith(home) else path

    # --- Geometry ---
    def size(self):
        s = shutil.get_terminal_size((80, 24))
        return s.columns, s.lines

    @property
    def width(self):
        return max(40, min(self.size()[0] - 1, 76))

    # --- Wizard state ---
    def answer(self, label, value):
        for a in self.answers:
            if a[0] == label:
                a[1] = value
                return
        self.answers.append([label, value])

    def drop(self, label):
        self.answers = [a for a in self.answers if a[0] != label]

    def set_done(self, n, idle=False):
        self.done = n
        self.idle = idle

    def note(self, msg, kind='info'):
        if self.interactive:
            self.notes.append((kind, msg))
        else:
            print(msg, file=sys.stderr if kind == 'warn' else sys.stdout)

    # --- Static pieces ---
    def _gap(self, start, end, grid):
        return ''.join(self._sgr('38;5;236', '+') if grid and c % 8 == 0 else ' ' for c in range(start, end))

    def _row(self, segments, grid=False):
        """Places (column, text) segments on one row, filling gaps with spaces or a faint '+' grid."""
        out, cur = '', 0
        for col, text in sorted(segments, key=lambda seg: seg[0]):
            out += self._gap(cur, col, grid) + text
            cur = col + _vlen(text)
        return out + self._gap(cur, self.width, grid)

    def _token_chip(self):
        mark = self.acc('✓') if os.environ.get('TOKEN') else self.dim('—')
        return f"{self.acc('[')} {self.dim('TOKEN')} {mark} {self.acc(']')}"

    def _feeds_chip(self):
        return f"{self.acc('[')} {self.bold('FEEDS')} {self.acc(']')}"

    def _wordmark_row(self, line):
        out = ''
        for is_block, group in itertools.groupby(line, key=lambda ch: ch == '█'):
            out += self._sgr('38;5;190' if is_block else '38;5;64', ''.join(group))
        return out

    def header_poster(self):
        """Launch-screen header: block wordmark, tag, and tagline."""
        lines = [self._row([], True)]
        for i, row in enumerate(self.WORDMARK):
            segs = [(2, self._wordmark_row(row))]
            if i == 0:
                segs.append((self.width - 13, self._token_chip()))
            elif i == 2:
                segs.append((38, self._feeds_chip()))
            elif i == 4:
                segs.append((38, "Download. Uncompress. Filter."))
            elif i == 5:
                segs.append((38, self.dim("The truth, hiding in plain sight.")))
            lines.append(self._row(segs, grid=(i % 2 == 1)))
        return lines

    def header_nav(self, size):
        """In-flow header: nav bar with brand mark and a staggered tagline ('full' > 'slim' > 'mini')."""
        token_col = self.width - 13
        buttons = [(token_col - 12, self._feeds_chip()), (token_col, self._token_chip())]
        if size == 'full':
            return [
                self._row([(2, self.acc('╲│╱'))], True),
                self._row([(2, self.acc('─┼─')), (7, self.bold('Spur')), (12, self.acc('Feeds'))] + buttons),
                self._row([(2, self.acc('╱│╲'))], True),
                self._row([]),
                self._row([(2, self.bold('Download, uncompress,'))], True),
                self._row([(14, self.bold('and filter Spur feeds'))]),
                self._row([], True),
            ]
        nav = self._row([(2, self.acc('✱')), (4, self.bold('Spur')), (9, self.acc('Feeds'))] + buttons)
        if size == 'mini':
            return [nav, self._row([])]
        return [
            nav,
            self._row([(2, self.bold('Download, uncompress,'))], True),
            self._row([(14, self.bold('and filter Spur feeds'))]),
            self._row([], True),
        ]

    def rail_poster(self):
        out = ''
        for i, name in enumerate(self.STEPS):
            label = name.upper()
            if i < self.done:
                out += self.acc(f"[ ✓ {label} ]")
            elif i == self.done and not self.idle:
                out += self._sgr('1;38;5;235;48;5;190', f"[ ● {label} ]")
            else:
                out += self.dim(f"[ ○ {label} ]")
            if i < len(self.STEPS) - 1:
                out += (self.acc if i < self.done else self.dim)(' ── ')
        return [' ' * 2 + out]

    def rail_tabs(self):
        """Tab labels with a progress underline."""
        segs, col, progress_end = [], 2, 2
        for i, name in enumerate(self.STEPS):
            label = name.upper()
            active = i == self.done and not self.idle
            if i < self.done:
                glyph, text = self.acc('✓'), self.acc(label)
            elif active:
                glyph, text = self.acc('●'), self.bold(label)
            else:
                glyph, text = self.dim('○'), self.dim(label)
            segs.append((col, f"{glyph} {text}"))
            col += 2 + len(label)
            if i < self.done or active:
                progress_end = col
            col += 6
        if self.done >= len(self.STEPS):
            progress_end = self.width - 2
        underline = '  ' + self.acc('━' * (progress_end - 2)) + self.track('─' * max(0, self.width - 2 - progress_end))
        return [self._row(segs), underline]

    def rail_compact(self):
        out = ''
        for i, name in enumerate(self.STEPS):
            if i < self.done:
                out += self.acc('✓ ' + name)
            elif i == self.done and not self.idle:
                out += self.acc('●') + ' ' + self.bold(name)
            else:
                out += self.dim('○ ' + name)
            if i < len(self.STEPS) - 1:
                out += (self.acc if i < self.done else self.dim)(' ── ')
        return [out]

    def _layout(self):
        """Chooses the header style for the current terminal size and returns (header_lines, rail_fn)."""
        rows, width = self.size()[1], self.width
        if rows < 20 or width < 56:
            self.rail_mode = 'compact'
            return [], self.rail_compact
        if self.launch and rows >= 22 and width >= 72:
            self.rail_mode = 'poster'
            return self.header_poster() + [''], self.rail_poster
        self.rail_mode = 'nav'
        return self.header_nav('full' if rows >= 32 else 'slim' if rows >= 28 else 'mini'), self.rail_tabs

    def rail_lines(self):
        return {'poster': self.rail_poster, 'nav': self.rail_tabs}.get(self.rail_mode, self.rail_compact)()

    def base_lines(self):
        header, rail_fn = self._layout()
        rail = rail_fn()
        lines = header + rail + ['']
        self.rail_row = len(header) + 1  # 1-based screen row of the first rail line
        if self.answers:
            for label, value in self.answers:
                lines.append(f"{self.acc('✓')} {self.dim(label.ljust(10))} {value}")
            lines.append('')
        self.launch = False
        return lines

    def screen(self):
        """Clears the terminal and draws the header, step rail, and answer log."""
        if not self.interactive:
            sys.stdout.write('\n')
            sys.stdout.flush()
            return
        lines = self.base_lines()
        for kind, msg in self.notes:
            lines.append(f"{self.amber('!') if kind == 'warn' else self.dim('·')} {msg}")
        if self.notes:
            lines.append('')
        self.notes = []
        sys.stdout.write('\x1b[2J\x1b[H' + '\n'.join(lines) + '\n')
        sys.stdout.flush()
        self.lines_above = len(lines)

    def refresh_rail(self):
        """Redraws the step rail in place (e.g. to show every step complete at the end)."""
        if self.interactive and self.lines_above + 2 < self.size()[1]:
            rail = ''.join(f'\x1b[{self.rail_row + i};1H{line}\x1b[K' for i, line in enumerate(self.rail_lines()))
            sys.stdout.write('\x1b7' + rail + '\x1b8')
            sys.stdout.flush()

    # --- Plain output ---
    def out(self, s='', err=False):
        stream = sys.stderr if err else sys.stdout
        stream.write(s + '\n')
        stream.flush()
        self.lines_above += 1

    def heading(self, title):
        self.out(f"{self.acc('◆')} {self.bold(title)}")
        self.out()

    def ok(self, msg):
        self.out(f"{self.acc('✓')} {msg}")

    def info(self, msg):
        self.out(f"  {self.dim(msg)}")

    def warn(self, msg):
        self.out(f"{self.amber('!')} {msg}", err=True)

    def error(self, msg):
        self.out(f"{self.red('✕')} {msg}", err=True)

    def columns(self, values, indent=2):
        """Lays out short strings in dim columns that fit the screen; truncates if there are too many."""
        if not values:
            return []
        cell = max(8, min(30, max(len(v) for v in values) + 2))
        per_row = max(1, (self.width - indent) // cell)
        rows = [values[i:i + per_row] for i in range(0, len(values), per_row)]
        max_lines = max(4, self.size()[1] - self.lines_above - 10)
        lines = [' ' * indent + self.dim(''.join((v if len(v) <= cell - 2 else v[:cell - 3] + '…').ljust(cell) for v in r)) for r in rows[:max_lines]]
        hidden = sum(len(r) for r in rows[max_lines:])
        if hidden:
            lines.append(' ' * indent + self.dim(f"… and {hidden:,} more not shown"))
        return lines

    # --- Progress ---
    def file_start(self, label):
        self.out(f"  {label}")

    def file_done(self, label, detail=''):
        if self.interactive:
            sys.stdout.write('\r\x1b[K\x1b[A\r\x1b[K' + f"{self.acc('✓')} {self.acc(label)}  {self.dim(detail)}" + '\n')
        else:
            sys.stdout.write(f"\nOK {label}  {detail}\n")
        sys.stdout.flush()

    def progress(self, pct, detail='', width=30):
        pct = max(0.0, min(100.0, pct))
        if self.interactive:
            cols = self.size()[0] - 1
            if len(detail) > cols - (width + 9):  # narrow terminal: shrink the bar, then clip the detail
                width = max(10, cols - 9 - len(detail))
                room = max(0, cols - (width + 9))
                detail = detail if len(detail) <= room else detail[:max(0, room - 1)] + '…'
            filled = int(width * pct / 100)
            bar = self.acc('━' * filled) + self.track('━' * (width - filled))
            sys.stdout.write('\r' + f"  {bar} {self.bold(f'{pct:3.0f}%')}  {self.dim(detail)}" + '\x1b[K')
        else:
            sys.stdout.write(f"\r  {pct:5.1f}%  {detail}")
        sys.stdout.flush()

    def spin(self, detail):
        if self.interactive:
            frame = self.SPINNER[int(time.time() * 10) % len(self.SPINNER)]
            detail = _clip(detail, max(10, self.size()[0] - 6))
            sys.stdout.write('\r' + f"  {self.acc(frame)} {self.dim(detail)}" + '\x1b[K')
        else:
            sys.stdout.write(f"\r  {detail}")
        sys.stdout.flush()

    def end_progress(self):
        sys.stdout.write('\r\x1b[K' if self.interactive else '\n')
        sys.stdout.flush()

    # --- Keyboard ---
    @contextlib.contextmanager
    def _keys(self, show_cursor=False):
        fd = sys.stdin.fileno()
        old = None
        if termios:
            old = termios.tcgetattr(fd)
            tty.setcbreak(fd)
        sys.stdout.write('\x1b[?25h' if show_cursor else '\x1b[?25l')
        sys.stdout.flush()
        try:
            yield
        finally:
            if old is not None:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            sys.stdout.write('\x1b[?25h')
            sys.stdout.flush()

    @staticmethod
    def _map_char(ch):
        if ch in ('\r', '\n'):
            return 'enter'
        if ch in ('\x7f', '\x08'):
            return 'backspace'
        if ch == '\t':
            return 'tab'
        if ch == '\x03':
            raise KeyboardInterrupt
        if ch == '\x01':
            return 'home'
        if ch == '\x05':
            return 'end'
        if ch == '\x15':
            return 'kill'
        if ch < ' ':
            return ''
        return ch

    def _read_key(self):
        if msvcrt:
            ch = msvcrt.getwch()
            if ch in ('\x00', '\xe0'):
                return {'H': 'up', 'P': 'down', 'K': 'left', 'M': 'right', 'G': 'home', 'O': 'end', 'S': 'delete'}.get(msvcrt.getwch(), '')
            return self._map_char(ch)
        fd = sys.stdin.fileno()
        b = os.read(fd, 1)
        if not b:
            raise EOFError
        if b == b'\x1b':
            if not _select.select([fd], [], [], 0.05)[0]:
                return 'esc'
            b2 = os.read(fd, 1)
            if b2 in (b'[', b'O'):
                seq = b''
                while True:
                    c = os.read(fd, 1)
                    seq += c
                    if not c or 0x40 <= c[0] <= 0x7e:
                        break
                return {b'A': 'up', b'B': 'down', b'C': 'right', b'D': 'left', b'H': 'home', b'F': 'end',
                        b'3~': 'delete', b'1~': 'home', b'4~': 'end'}.get(seq, '')
            return 'esc'
        if b[0] >= 0xC0:
            need = 1 if b[0] < 0xE0 else 2 if b[0] < 0xF0 else 3
            while need > 0:
                b += os.read(fd, 1)
                need -= 1
            return b.decode('utf-8', 'ignore')
        return self._map_char(b.decode('latin-1'))

    def _pending(self):
        if msvcrt:
            return msvcrt.kbhit()
        return bool(_select.select([sys.stdin.fileno()], [], [], 0)[0])

    def _read_keys(self):
        """Blocks for one key, then drains anything already queued (e.g. a paste)."""
        keys = [self._read_key()]
        while self._pending():
            keys.append(self._read_key())
        return keys

    # --- Widget plumbing ---
    def _begin(self, clear, body):
        if clear:
            self.screen()
        for line in body or []:
            self.out(line)
        if body:
            self.out()

    def _prompt_line(self, title, hint=None):
        left = f"{self.acc('◆')} {self.bold(title)}"
        if hint:
            gap = self.width - _vlen(left) - len(hint)
            if gap >= 2:
                return left + ' ' * gap + self.dim(hint)
        return left

    def _draw(self, lines, prev_h, below=0):
        cols = self.size()[0]
        out = ''
        if prev_h:
            if below:
                out += f'\x1b[{below}B'
            out += '\r'
            if prev_h > 1:
                out += f'\x1b[{prev_h - 1}A'
        out += '\n'.join(_clip(l, cols - 1) + '\x1b[K' for l in lines) + '\x1b[J'
        sys.stdout.write(out)
        sys.stdout.flush()
        return len(lines)

    def _footer(self, text, height):
        cols, rows = self.size()
        if self.lines_above + height + 2 >= rows:
            return
        cwd = self.tilde(os.getcwd())
        if len(text) + 2 + len(cwd) > cols - 1:
            cwd = '…' + os.sep + os.sep.join(cwd.split(os.sep)[-2:])
        left, right = self.dim(text), self.dim(cwd)
        gap = cols - 1 - _vlen(left) - _vlen(right)
        line = left + ' ' * gap + right if gap >= 2 else left
        sys.stdout.write(f'\x1b7\x1b[{rows};1H\x1b[2K{line}\x1b8')
        sys.stdout.flush()

    def _finish(self, height, below=0):
        rows = self.size()[1]
        sys.stdout.write((f'\x1b[{below}B' if below else '') + '\n' + f'\x1b7\x1b[{rows};1H\x1b[2K\x1b8')
        sys.stdout.flush()
        self.lines_above += height

    @staticmethod
    def _window(n_rows, cursor_row, offset, avail):
        if n_rows <= avail:
            return 0, n_rows
        visible = max(3, avail - 2)
        if cursor_row < offset:
            offset = cursor_row
        elif cursor_row >= offset + visible:
            offset = cursor_row - visible + 1
        return offset, min(n_rows, offset + visible)

    def _item(self, name, selected):
        if selected:
            text = f" ❯ ◉  {name}"
            return self._sgr('1;38;5;190;48;5;236', text + ' ' * max(0, self.width - len(text)))
        return f"   {self.dim('○')}  {self.dim(name)}"

    # --- Widgets ---
    def select(self, title, options, subtitle=None, sep_before=None, exit_keys=(), clear=True, body=None, hint="↑↓ move   ⏎ select"):
        """Arrow-key list. Returns the chosen index, or a key name from exit_keys ('esc' / 'q')."""
        if not self.interactive:
            return self._plain_select(title, options, exit_keys)
        self._begin(clear, body)
        rows = []
        for i in range(len(options)):
            if sep_before == i:
                rows.append(None)
            rows.append(i)
        footer = "↑↓ move   1-9 jump   ⏎ select" + ("   esc back" if 'esc' in exit_keys else "") + ("   q quit" if 'q' in exit_keys else "")
        cursor, offset, prev_h, result = 0, 0, 0, None
        with self._keys():
            while True:
                head = [self._prompt_line(title, hint)]
                if subtitle:
                    head.append('  ' + self.dim(subtitle))
                head.append('')
                avail = max(5, self.size()[1] - self.lines_above - len(head) - 3)
                offset, end = self._window(len(rows), rows.index(cursor), offset, avail)
                lines = list(head)
                if offset > 0:
                    lines.append('   ' + self.dim(f"↑ {offset} more"))
                for r in rows[offset:end]:
                    lines.append('   ' + self.rule('─' * 28) if r is None else self._item(options[r], r == cursor))
                if end < len(rows):
                    lines.append('   ' + self.dim(f"↓ {len(rows) - end} more"))
                prev_h = self._draw(lines, prev_h)
                self._footer(footer, prev_h)
                for key in self._read_keys():
                    if key == 'up':
                        cursor = (cursor - 1) % len(options)
                    elif key == 'down':
                        cursor = (cursor + 1) % len(options)
                    elif key == 'enter':
                        result = cursor
                    elif key in exit_keys:
                        result = key
                    elif len(key) == 1 and key in '123456789' and int(key) <= len(options):
                        cursor = int(key) - 1
                    if result is not None:
                        break
                if result is not None:
                    break
        self._finish(prev_h)
        return result

    def confirm(self, title, default=True, subtitle=None, clear=True, body=None, labels=("Yes", "No")):
        """Yes/No toggle. Returns True for the first label."""
        if not self.interactive:
            return self._plain_confirm(title, default)
        self._begin(clear, body)
        cursor = 0 if default else 1
        prev_h, result = 0, None
        with self._keys():
            while True:
                lines = [self._prompt_line(title, "←→ move   ⏎ select")]
                if subtitle:
                    lines.append('  ' + self.dim(subtitle))
                lines.append('')
                pills = ''
                for i, name in enumerate(labels):
                    text = f"  {name}  "
                    pills += '  ' + (self._sgr('1;38;5;235;48;5;190', text) if i == cursor else self.dim(text))
                lines.append(pills)
                prev_h = self._draw(lines, prev_h)
                self._footer("←→ move   y / n answer   ⏎ select", prev_h)
                for key in self._read_keys():
                    if key in ('left', 'right', 'tab', 'h', 'l'):
                        cursor = 1 - cursor
                    elif key in ('y', 'Y'):
                        result = True
                    elif key in ('n', 'N'):
                        result = False
                    elif key == 'enter':
                        result = (cursor == 0)
                    if result is not None:
                        break
                if result is not None:
                    break
        self._finish(prev_h)
        return result

    def text(self, title, hint=None, default=None, validate=None, subtitle=None, clear=True, body=None):
        """Single-line input box. Empty input returns `default` (or ''). `validate` returns an error string or None."""
        if not self.interactive:
            return self._plain_text(title, default, validate, subtitle, body)
        self._begin(clear, body)
        boxw = min(self.width, 64)
        inner = boxw - 4
        buf, pos, error = '', 0, None
        prev_h, below, result = 0, 0, None
        with self._keys(show_cursor=True):
            while True:
                start = max(0, pos - inner + 1) if len(buf) >= inner else 0
                if buf:
                    view = buf[start:start + inner]
                    shown = view + ' ' * (inner - len(view))
                else:
                    shown = self.dim((default or '')[:inner].ljust(inner)) if default else ' ' * inner
                lines = [self._prompt_line(title, hint)]
                if subtitle:
                    lines.append('  ' + self.dim(subtitle))
                lines.append('')
                lines.append('  ' + self.acc('╭' + '─' * (inner + 2) + '╮'))
                input_row = len(lines)
                lines.append('  ' + self.acc('│') + ' ' + shown + ' ' + self.acc('│'))
                lines.append('  ' + self.acc('╰' + '─' * (inner + 2) + '╯'))
                if error:
                    lines.append('  ' + self.red('✕ ' + error))
                prev_h = self._draw(lines, prev_h, below)
                self._footer("⏎ confirm" + ("   empty uses the default" if default else ""), prev_h)
                below = len(lines) - 1 - input_row
                sys.stdout.write((f'\x1b[{below}A' if below else '') + f'\x1b[{5 + pos - start}G')
                sys.stdout.flush()
                for key in self._read_keys():
                    if key == 'enter':
                        value = buf.strip() or (default or '')
                        err = validate(value) if validate else None
                        if err:
                            error = err
                        else:
                            result = value
                            break
                    elif key == 'backspace':
                        if pos > 0:
                            buf, pos, error = buf[:pos - 1] + buf[pos:], pos - 1, None
                    elif key == 'delete':
                        buf, error = buf[:pos] + buf[pos + 1:], None
                    elif key == 'left':
                        pos = max(0, pos - 1)
                    elif key == 'right':
                        pos = min(len(buf), pos + 1)
                    elif key == 'home':
                        pos = 0
                    elif key == 'end':
                        pos = len(buf)
                    elif key == 'kill':
                        buf, pos, error = '', 0, None
                    elif len(key) == 1 and key.isprintable():
                        buf, pos, error = buf[:pos] + key + buf[pos:], pos + 1, None
                if result is not None:
                    break
        self._finish(prev_h, below)
        return result

    def search_select(self, title, options, subtitle=None, clear=True, body=None):
        """Type-to-filter list. Returns the chosen option, or the typed text if nothing matches."""
        if not self.interactive:
            return self._plain_search(title, options, subtitle)
        self._begin(clear, body)
        lowered = [o.lower() for o in options]
        query, cursor, offset, prev_h, result = '', 0, 0, 0, None
        with self._keys():
            while True:
                matches = [i for i, o in enumerate(lowered) if query.lower() in o]
                cursor = min(cursor, max(0, len(matches) - 1))
                head = [self._prompt_line(title, "↑↓ move   ⏎ select")]
                if subtitle:
                    head.append('  ' + self.dim(subtitle))
                head.append('')
                head.append('  ' + self.acc('/') + ' ' + (query + self.acc('▌') if query else self.dim('type to filter')))
                head.append('')
                avail = max(5, self.size()[1] - self.lines_above - len(head) - 3)
                offset, end = self._window(len(matches), cursor, offset, avail)
                lines = list(head)
                if matches:
                    if offset > 0:
                        lines.append('   ' + self.dim(f"↑ {offset} more"))
                    for m in range(offset, end):
                        lines.append(self._item(options[matches[m]], m == cursor))
                    if end < len(matches):
                        lines.append('   ' + self.dim(f"↓ {len(matches) - end} more"))
                else:
                    lines.append('   ' + self.dim(f"No match. ⏎ uses '{query}' exactly as typed."))
                prev_h = self._draw(lines, prev_h)
                self._footer("type to filter   ↑↓ move   ⏎ select   esc clear", prev_h)
                for key in self._read_keys():
                    if key == 'up' and matches:
                        cursor = (cursor - 1) % len(matches)
                    elif key == 'down' and matches:
                        cursor = (cursor + 1) % len(matches)
                    elif key == 'enter':
                        if matches:
                            result = options[matches[cursor]]
                        elif query:
                            result = query
                    elif key == 'backspace':
                        query, cursor = query[:-1], 0
                    elif key in ('esc', 'kill'):
                        query, cursor = '', 0
                    elif len(key) == 1 and key.isprintable():
                        query, cursor = query + key, 0
                    if result is not None:
                        break
                if result is not None:
                    break
        self._finish(prev_h)
        return result

    # --- Plain-mode fallbacks (no TTY, TERM=dumb, or SPUR_PLAIN=1) ---
    def _plain_select(self, title, options, exit_keys=()):
        print(f"\n{title}")
        for i, name in enumerate(options, start=1):
            print(f"  {i}. {name}")
        letters = [k for k in exit_keys if len(k) == 1]
        while True:
            raw = input(f"Select (1-{len(options)}): ").strip()
            if raw.isdigit() and 1 <= int(raw) <= len(options):
                return int(raw) - 1
            if raw.lower() in letters:
                return raw.lower()
            print("Invalid choice. Please try again.", file=sys.stderr)

    def _plain_confirm(self, title, default):
        while True:
            raw = input(f"{title} (Y/N) [Default: {'Y' if default else 'N'}]: ").strip().upper()
            if raw == '':
                return default
            if raw in ('Y', 'N'):
                return raw == 'Y'
            print("Invalid response. Please enter Y or N.", file=sys.stderr)

    def _plain_text(self, title, default, validate, subtitle, body):
        for line in ([subtitle] if subtitle else []) + list(body or []):
            print(line)
        suffix = f" [Default: {default}]" if default else ''
        while True:
            raw = input(f"{title}{suffix}: ").strip()
            if not raw and default is not None:
                raw = default
            err = validate(raw) if validate else None
            if err:
                print(err, file=sys.stderr)
                continue
            return raw

    def _plain_search(self, title, options, subtitle):
        print(f"\n{subtitle or title}")
        for name in options:
            print(f"  - {name}")
        return input(f"{title} (type the exact key name): ").strip()


ui = UI()

# --- Functions ---
# --- Helper Functions ---

def flatten_json(json_data, parent_key='', sep='_'):
    """Flattens a nested JSON object into a single dictionary."""
    items = []
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(not isinstance(item, dict) for item in v):
                items.append((new_key, ','.join(map(str, v))))
            else:
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_json(item, new_key + sep + str(i), sep=sep).items())
                    else:
                        items.append((new_key + sep + str(i), item))
        else:
            items.append((new_key, v))
    return dict(items)

def get_output_filename(current_date_ymd, current_time_hms, base_feed_name, user_filename, filter_criteria, overall_match_type):
    """Determines the output filename."""
    if user_filename:
        sanitized_filename = "".join(x for x in user_filename if x.isalnum() or x in "._-")
        if not sanitized_filename.lower().endswith(".json"):
            sanitized_filename += ".json"
        return sanitized_filename

    filename_parts = [current_date_ymd]
    
    if (base_feed_name == "AnonResRT" or base_feed_name == "AnonymousResidentialRT") and current_time_hms:
        filename_parts.append(current_time_hms)
        
    if base_feed_name.endswith('Hist'):
        base_feed_name = base_feed_name[:-4]

    filename_parts.append(base_feed_name)

    if filter_criteria:
        for i, criterion in enumerate(filter_criteria):
            key_name = criterion['key']
            kws = criterion['keywords']

            if key_name:
                sanitized_key_name = re.sub(r'[^a-zA-Z0-9]', '', key_name).title()
                if sanitized_key_name:
                    filename_parts.append(sanitized_key_name)
            
            if kws:
                sanitized_keywords = [re.sub(r'[^a-zA-Z0-9]', '', kw).title() for kw in kws][:3]
                if sanitized_keywords:
                    filename_parts.append("".join(sanitized_keywords))
            
    if len(filter_criteria) > 1:
        filename_parts.append(overall_match_type.upper())

    default_filename = "".join(filename_parts) + ".json"
    return default_filename

def get_file_chunks(filepath, min_chunks):
    """
    Determines byte offsets for splitting a file.
    OPTIMIZATION: Targets ~64MB chunks to prevent memory bloat in workers.
    """
    file_size = os.path.getsize(filepath)
    
    # Target chunk size: 64MB (safe for memory even with 100% match rate)
    TARGET_CHUNK_SIZE = 64 * 1024 * 1024
    
    # Calculate required chunks based on size, but ensure at least 'min_chunks' (CPU count)
    size_based_chunks = (file_size // TARGET_CHUNK_SIZE) + 1
    num_chunks = max(min_chunks, size_based_chunks)
    
    chunk_size = file_size // num_chunks
    
    chunks = []
    with open(filepath, 'rb') as f:
        for i in range(num_chunks):
            start_byte = i * chunk_size
            end_byte = (i + 1) * chunk_size if i < num_chunks - 1 else file_size
            
            if start_byte > 0:
                f.seek(start_byte - 1)
                f.readline() 
                start_byte = f.tell()
            
            if start_byte < end_byte:
                chunks.append((start_byte, end_byte))
                
    return chunks

def evaluate_match(json_obj, filter_criteria, overall_match_type, raw_line_for_general_search=""):
    """Evaluates a single JSON object against all filter criteria."""
    # Evaluate each filter criterion
    criterion_results = []
    for criterion in filter_criteria:
        key_name = criterion['key']
        kws = criterion['keywords']
        match_type_keywords = criterion['match_type_keywords']

        flattened_obj = flatten_json(json_obj)
        
        source_value_raw = None
        
        # --- Determine Source Value ---
        if key_name and key_name.startswith('tunnels_'):
            sub_key = key_name.split('_', 1)[1]
            relevant_keys = [k for k in flattened_obj.keys() if k.startswith('tunnels_') and k.endswith(f'_{sub_key}')]
            
            if relevant_keys:
                source_values = [str(flattened_obj[k]) for k in relevant_keys if k in flattened_obj and str(flattened_obj[k]).strip().lower() not in ('none', 'null')]
                source_value_raw = ','.join(source_values)
            
        elif key_name:
            if key_name in flattened_obj:
                value = flattened_obj.get(key_name)
                if value is not None:
                    str_value = str(value).strip().lower()
                    if str_value not in ('none', 'null'):
                        source_value_raw = str_value
            
        else: # General search
            source_value_raw = raw_line_for_general_search.lower()

        # --- Evaluate Keywords ---
        current_kws_match_status = True if match_type_keywords == 'AND' else False
        
        for kw in kws:
            individual_match = False
            
            # Handle Negation Logic (!)
            is_negation = False
            clean_kw = kw
            
            if kw.startswith('!') and kw != '!=empty':
                is_negation = True
                clean_kw = kw[1:]

            # 1. Special case: EMPTY / NOT EMPTY
            if clean_kw == '=empty':
                if not source_value_raw or not source_value_raw.strip():
                    individual_match = True
            elif clean_kw == '!=empty':
                if source_value_raw and source_value_raw.strip():
                    individual_match = True
            
            # 2. Standard substring/numerical filtering
            elif source_value_raw:
                val_str = str(source_value_raw).lower()
                
                # Numerical Check
                num_match = re.match(r'([<>]?=?)\s*(\-?\d+(\.\d+)?)$', clean_kw, re.IGNORECASE)
                
                if num_match:
                    operator = num_match.group(1)
                    target_num_str = num_match.group(2)
                    try:
                        target_num = float(target_num_str)
                        actual_num = float(val_str)
                        
                        if operator == '>' and actual_num > target_num: individual_match = True
                        elif operator == '<' and actual_num < target_num: individual_match = True
                        elif operator == '>=' and actual_num >= target_num: individual_match = True
                        elif operator == '<=' and actual_num <= target_num: individual_match = True
                        elif (operator == '=' or operator == '') and actual_num == target_num: individual_match = True
                    except (ValueError, TypeError):
                        pass 
                else:
                    if clean_kw in val_str:
                        individual_match = True
            
            if is_negation:
                individual_match = not individual_match

            if match_type_keywords == 'AND':
                if not individual_match:
                    current_kws_match_status = False
                    break
            elif match_type_keywords == 'OR':
                if individual_match:
                    current_kws_match_status = True
                    break
        
        criterion_results.append(current_kws_match_status)

    if not criterion_results:
        return False

    return all(criterion_results) if overall_match_type == 'AND' else any(criterion_results)

def process_file_chunk(args_tuple):
    """
    Processes a specific byte range (chunk) of a file.
    OPTIMIZATION: Returns raw strings (line_stripped) instead of dict objects.
    """
    filepath, start_byte, end_byte, filter_criteria, overall_match_type = args_tuple
    
    matching_lines = [] 
    lines_parsed = 0
    
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        f.seek(start_byte)
        
        current_byte = start_byte
        for line in f:
            lines_parsed += 1
            line_stripped = line.strip()
            current_byte += len(line.encode('utf-8'))
            
            if not line_stripped:
                if current_byte >= end_byte:
                    break
                continue
            
            try:
                json_obj = json.loads(line_stripped)
                if evaluate_match(json_obj, filter_criteria, overall_match_type, line_stripped):
                    # OPTIMIZATION: Store the raw string, not the dict object
                    matching_lines.append(line_stripped)

            except json.JSONDecodeError:
                pass 
            except Exception as e:
                pass
            
            if current_byte >= end_byte:
                break
    return matching_lines, lines_parsed

def process_mmdb_and_filter(mmdb_path, output_path, filter_criteria, overall_match_type):
    """
    Opens an MMDB file, iterates through it, filters records, and writes matches to a JSONL file.
    Returns the count of exported records and total records parsed.
    """
    records_exported = 0
    records_parsed = 0
    start_time = time.time()
    last_update_time = start_time

    try:
        ui.info(f"Opening MMDB file: {mmdb_path}")
        with maxminddb.open_database(mmdb_path) as reader, open(output_path, 'w', encoding='utf-8') as outfile:
            
            ui.info("Iterating through MMDB (this may take a while for large files)")
            
            # The iterator doesn't have a __len__, so we count as we go for progress.
            for network, data in reader:
                records_parsed += 1
                
                # For general search, we need a string representation of the record
                raw_line_for_general_search = json.dumps(data)

                if evaluate_match(data, filter_criteria, overall_match_type, raw_line_for_general_search):
                    # Add network info to the record before writing
                    data['network'] = str(network)
                    outfile.write(json.dumps(data) + '\n')
                    records_exported += 1

                # UI Update throttling
                current_time = time.time()
                if current_time - last_update_time >= 0.2:
                    elapsed_time = current_time - start_time
                    records_per_second = records_parsed / elapsed_time if elapsed_time > 0 else 0
                    ui.spin(f"Parsed {records_parsed:,} records · Exported {records_exported:,} · {records_per_second:,.0f} records/s")
                    last_update_time = current_time
            
            ui.end_progress()

    except Exception as e:
        ui.end_progress()
        ui.error(f"Error processing MMDB file: {e}")
        return 0, 0
        
    return records_exported, records_parsed

def decompress_file(gzipped_path):
    """Decompresses a .gz file, removes the original, and returns the new path."""
    # Correctly remove .gz suffix, e.g. "file.json.gz" becomes "file.json"
    decompressed_file_path = gzipped_path.rsplit('.gz', 1)[0]
    label = os.path.basename(gzipped_path)

    ui.file_start(label)
    try:
        total_compressed = os.path.getsize(gzipped_path) or 1
        written = 0
        start_time = last_update_time = time.time()
        with open(gzipped_path, 'rb') as raw, gzip.GzipFile(fileobj=raw) as f_in, open(decompressed_file_path, 'wb') as f_out:
            while True:
                chunk = f_in.read(4 * 1024 * 1024)
                if not chunk:
                    break
                f_out.write(chunk)
                written += len(chunk)
                now = time.time()
                if now - last_update_time >= 0.2:
                    ui.progress(raw.tell() / total_compressed * 100, f"{_fmt_bytes(written)} written")
                    last_update_time = now

        ui.file_done(os.path.basename(decompressed_file_path), f"{_fmt_bytes(written)} · {_fmt_secs(time.time() - start_time)}")

        try:
            os.remove(gzipped_path)
            ui.info(f"Removed {label}")
        except OSError as e:
            ui.error(f"Error deleting gzipped file {gzipped_path}: {e}")

        return decompressed_file_path
    except Exception as e:
        ui.end_progress()
        ui.error(f"Error during decompression: {e}")
        return None

def download_with_resume(url, token, output_path, max_retries=10, connect_timeout=15, stall_timeout=30, label=None):
    """
    Downloads a file to output_path, automatically resuming after network
    interruptions using HTTP Range requests.

    A per-chunk read timeout (stall_timeout) is used to detect a connection
    that has gone silent (no bytes arriving, no error raised) rather than
    hanging forever with no visible progress. On any network error the
    partial file is kept on disk and the download is retried (with backoff),
    resuming from the last byte written. If every retry is exhausted, the
    partial file is still left in place so a later re-run can pick up where
    this one left off.
    """
    label = label or os.path.basename(output_path)
    ui.file_start(label)
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        resume_from = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        headers = {"Token": token}
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"

        try:
            response = requests.get(url, headers=headers, stream=True, timeout=(connect_timeout, stall_timeout))

            if response.status_code == 416:
                # Server says the range we already have covers the whole file.
                response.close()
                ui.file_done(label, "already complete")
                return output_path

            resuming = resume_from > 0 and response.status_code == 206
            restarted = resume_from > 0 and not resuming

            response.raise_for_status()

            content_range = response.headers.get('content-range')
            if content_range and '/' in content_range:
                total_size = int(content_range.rsplit('/', 1)[-1])
            else:
                total_size = (resume_from if resuming else 0) + int(response.headers.get('content-length', 0))

            downloaded_size = resume_from if resuming else 0
            session_start_size = downloaded_size
            start_time = time.time()
            last_update_time = 0

            with open(output_path, 'ab' if resuming else 'wb') as outfile:
                for chunk in response.iter_content(chunk_size=8192):
                    outfile.write(chunk)
                    downloaded_size += len(chunk)
                    current_time = time.time()
                    if current_time - last_update_time >= 0.25:
                        elapsed_time = current_time - start_time
                        rate = (downloaded_size - session_start_size) / elapsed_time if elapsed_time else 0
                        if total_size:
                            detail = f"{_fmt_bytes(downloaded_size)} / {_fmt_bytes(total_size)} · {_fmt_bytes(rate)}/s"
                            if rate:
                                detail += f" · {_fmt_secs((total_size - downloaded_size) / rate)} left"
                            if resuming:
                                detail += " · resumed"
                            ui.progress(downloaded_size / total_size * 100, detail)
                        else:
                            ui.spin(f"{_fmt_bytes(downloaded_size)} · {_fmt_bytes(rate)}/s")
                        last_update_time = current_time

            detail = f"{_fmt_bytes(downloaded_size)} · {_fmt_secs(time.time() - start_time)}"
            if restarted:
                detail += " · server ignored resume, restarted"
            ui.file_done(label, detail)
            return output_path

        except (requests.exceptions.RequestException, OSError) as e:
            ui.end_progress()
            wait_time = min(60, 5 * attempt)
            ui.warn(f"Network error ({e}). Retrying in {wait_time}s... (attempt {attempt}/{max_retries})")
            if attempt < max_retries:
                time.sleep(wait_time)
                ui.file_start(label)

    ui.error(f"Giving up after {max_retries} attempts. Partial file kept at '{output_path}' -- re-running will resume it.")
    return None

def download_and_decompress_gz_to_file(url, token, output_path):
    """Downloads (with automatic resume) and decompresses a .gz file."""
    print(f"Downloading from: {url}")
    if not download_with_resume(url, token, output_path):
        return None

    print(f"Successfully downloaded gzipped file to: {output_path}")

    decompressed_file_path = os.path.splitext(output_path)[0]
    if not decompressed_file_path.lower().endswith('.json'):
        decompressed_file_path += '.json'

    try:
        print(f"Decompressing {output_path} to {decompressed_file_path}...")

        with gzip.open(output_path, 'rb') as f_in:
            with open(decompressed_file_path, 'wb') as f_out:
                buffer_size = 1024 * 1024
                while True:
                    chunk = f_in.read(buffer_size)
                    if not chunk:
                        break
                    f_out.write(chunk)

        print(f"Successfully decompressed to {decompressed_file_path}")

        try:
            os.remove(output_path)
            print(f"Deleted temporary gzipped file: {output_path}")
        except OSError as e:
            print(f"Error deleting gzipped file {output_path}: {e}", file=sys.stderr)

        return decompressed_file_path
    except Exception as e:
        print(f"Error during decompression: {e}", file=sys.stderr)
        return None

def download_raw_file_to_disk(url, token, output_path):
    """Downloads a raw file, automatically resuming after network interruptions."""
    ui.info(f"from {url}")
    return download_with_resume(url, token, output_path)

def record_iterator(filepath, is_json, sample_size):
    """Yields records from a JSONL or MMDB file, up to sample_size."""
    count = 0
    if is_json:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if count >= sample_size:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                        count += 1
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            ui.warn(f"Error reading JSON file for sampling: {e}")
    else: # is MMDB
        try:
            with maxminddb.open_database(filepath) as reader:
                for _, data in reader:
                    if count >= sample_size: break
                    yield data
                    count += 1
        except Exception as e:
            ui.warn(f"Error reading MMDB file for sampling: {e}")

def generate_hhmm_range(start_hhmm, end_hhmm, step_minutes=5):
    """Generates a list of HHMM strings from start to end (inclusive), stepping by step_minutes."""
    start_dt = datetime.datetime.strptime(start_hhmm, "%H%M")
    end_dt = datetime.datetime.strptime(end_hhmm, "%H%M")
    times = []
    current_dt = start_dt
    while current_dt <= end_dt:
        times.append(current_dt.strftime("%H%M"))
        current_dt += datetime.timedelta(minutes=step_minutes)
    return times

def generate_date_range(start_ymd, end_ymd):
    """Generates a list of YYYYMMDD strings from start to end (inclusive)."""
    start_dt = datetime.datetime.strptime(start_ymd, "%Y%m%d")
    end_dt = datetime.datetime.strptime(end_ymd, "%Y%m%d")
    dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append(current_dt.strftime("%Y%m%d"))
        current_dt += datetime.timedelta(days=1)
    return dates

def download_realtime_range_files(url_template, date_ymd, hhmm_list, base_feed_name, token):
    """
    Downloads a series of 5-minute Realtime feed files, each kept as its own
    separate, still-gzipped .json.gz file (no merging, no decompression).
    Returns (saved_paths, failed_times).
    """
    total_files = len(hhmm_list)
    saved_paths = []
    failed_times = []

    for idx, hhmm in enumerate(hhmm_list, start=1):
        url = url_template.format(date_ymd, hhmm)
        output_path = f"{date_ymd}{hhmm}{base_feed_name}.json.gz"

        if download_with_resume(url, token, output_path, label=f"[{idx}/{total_files}] {output_path}"):
            saved_paths.append(output_path)
        else:
            failed_times.append(hhmm)

    if failed_times:
        ui.warn(f"{len(failed_times)}/{total_files} time slice(s) failed and were skipped: {', '.join(failed_times)}")

    return saved_paths, failed_times

def download_historical_date_range_files(url_template, date_list, base_feed_name, needs_decompression, output_ext, token):
    """
    Downloads a series of historical feed files, one per date in date_list.
    Each file is kept as its own, still-gzipped (or raw, for .mmdb) file --
    no merging, no decompression. Returns (saved_paths, failed_dates).
    """
    total_files = len(date_list)
    saved_paths = []
    failed_dates = []

    for idx, date_ymd in enumerate(date_list, start=1):
        url = url_template.format(date_ymd)
        ext = ".json.gz" if needs_decompression else output_ext
        output_path = f"{date_ymd}{base_feed_name}{ext}"

        if download_with_resume(url, token, output_path, label=f"[{idx}/{total_files}] {output_path}"):
            saved_paths.append(output_path)
        else:
            failed_dates.append(date_ymd)

    if failed_dates:
        ui.warn(f"{len(failed_dates)}/{total_files} date(s) failed and were skipped: {', '.join(failed_dates)}")

    return saved_paths, failed_dates

def keyword_tips_lines():
    """Keyword entry tips, shown above the keyword prompt."""
    return [
        ui.dim("  Keyword search tips"),
        ui.dim("  Text         words to find, e.g. malicious (case-insensitive)"),
        ui.dim("  Negation     prefix with ! to exclude, e.g. !clean"),
        ui.dim("  Existence    =EMPTY for missing values, !=EMPTY for present values"),
        ui.dim("  Numbers      >50, <=100, =443"),
        ui.dim("  Combine      separate keywords with commas, e.g. >50, !clean, !=EMPTY"),
    ]

# --- Feed Menu ---
MENU_STRUCTURE = {
    "1": {
        "category": "AI Data",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/ai/latest.json.gz", "base_feed_name": "AIData", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/ai/{}/feed.json.gz", "base_feed_name": "AIData", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
        }
    },
    "2": {
        "category": "Anonymous",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/anonymous/latest.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/anonymous/{}/feed.json.gz", "base_feed_name": "Anonymous", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "3": {"name": "IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-ipv6/latest.json.gz", "base_feed_name": "AnonymousIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "4": {"name": "IPv6 (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-ipv6/{}/feed.json.gz", "base_feed_name": "AnonymousIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "5": {"name": "MMDB (Latest)", "url": "https://feeds.spur.us/v2/anonymous/latest.mmdb", "base_feed_name": "Anonymous", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False, "is_json": False},
            "6": {"name": "MMDB (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous/{}/feed.mmdb", "base_feed_name": "Anonymous", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": True, "is_json": False},
        }
    },
    "3": {
        "category": "Anonymous+Residential",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.json.gz", "base_feed_name": "AnonymousResidential", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/anonymous-residential/{}/feed.json.gz", "base_feed_name": "AnonymousResidential", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "3": {"name": "IPv6 (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential-ipv6/latest.json.gz", "base_feed_name": "AnonymousResidentialIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "4": {"name": "IPv6 (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential-ipv6/{}/feed.json.gz", "base_feed_name": "AnonymousResidentialIPv6", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "5": {"name": "Realtime (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/realtime/latest.json.gz", "base_feed_name": "AnonResRT", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "6": {"name": "Realtime (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/realtime/{}/{}.json.gz", "base_feed_name": "AnonymousResidentialRT", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "7": {"name": "MMDB (Latest)", "url": "https://feeds.spur.us/v2/anonymous-residential/latest.mmdb", "base_feed_name": "AnonymousResidential", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False, "is_json": False},
            "8": {"name": "MMDB (Historical)", "url_template": "https://feeds.spur.us/v2/anonymous-residential/{}/feed.mmdb", "base_feed_name": "AnonymousResidential", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": True, "is_json": False},
        }
    },
    "4": {
        "category": "Datacenter Hosting",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/dch/latest.json.gz", "base_feed_name": "DCH", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/dch/{}/feed.json.gz", "base_feed_name": "DCH", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
        }
    },
    "5": {
        "category": "IP Geo",
        "options": {
            "1": {"name": "JSON Format (Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.json.gz", "base_feed_name": "IPGeoJSON", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "JSON Format (Historical)", "url_template": "https://feeds.spur.us/v2/ipgeo/{}/feed.json.gz", "base_feed_name": "IPGeoJSON", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
            "3": {"name": "MMDB Format (Latest)", "url": "https://feeds.spur.us/v2/ipgeo/latest.mmdb", "base_feed_name": "IPGeoMMDB", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": False, "is_json": False},
            "4": {"name": "MMDB Format (Historical)", "url_template": "https://feeds.spur.us/v2/ipgeo/{}/feed.mmdb", "base_feed_name": "IPGeoMMDB", "needs_decompression": False, "output_ext": ".mmdb", "is_historical": True, "is_json": False},
        }
    },
    "6": {
        "category": "IP Summary",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/ipsummary/latest.json.gz", "base_feed_name": "IPSummary", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/ipsummary/{}/feed.json.gz", "base_feed_name": "IPSummary", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
        }
    },
    "7": {
        "category": "Service Metrics",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v2/service-metrics/latest.json.gz", "base_feed_name": "ServiceMetrics", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
            "2": {"name": "Historical", "url_template": "https://feeds.spur.us/v2/service-metrics/{}/feed.json.gz", "base_feed_name": "ServiceMetrics", "needs_decompression": True, "output_ext": ".json", "is_historical": True, "is_json": True},
        }
    },
    "8": {
        "category": "Similar IPs",
        "options": {
            "1": {"name": "Latest", "url": "https://feeds.spur.us/v1/similar-ips/latest.json.gz", "base_feed_name": "SimilarIPs", "needs_decompression": True, "output_ext": ".json", "is_historical": False, "is_json": True},
        }
    }
}

# --- Main Script Logic ---
def validate_ymd(value):
    if not re.fullmatch(r'\d{8}', value):
        return "Invalid format. Use YYYYMMDD (e.g., 20260301)."
    try:
        datetime.datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return "Invalid date."
    return None

def validate_date_range(value):
    m = re.fullmatch(r'(\d{8})-(\d{8})', value)
    if not m:
        return "Invalid format. Use YYYYMMDD-YYYYMMDD (e.g., 20260101-20260107)."
    try:
        start = datetime.datetime.strptime(m.group(1), "%Y%m%d")
        end = datetime.datetime.strptime(m.group(2), "%Y%m%d")
    except ValueError:
        return "Invalid date."
    if start > end:
        return "Start date must be before or equal to end date."
    return None

def validate_hhmm(value):
    if not re.fullmatch(r'\d{4}', value):
        return "Invalid format. Use HHMM (e.g., 0000, 1345)."
    try:
        datetime.datetime.strptime(value, "%H%M")
    except ValueError:
        return "Invalid time value."
    return None

def validate_five_minute(value):
    err = validate_hhmm(value)
    if err:
        return err
    if int(value[2:]) % 5 != 0:
        return "Times must fall on 5-minute intervals (00, 05, ... 55); this feed is published every 5 minutes."
    return None

def validate_count(value):
    if value.isdigit() and int(value) > 0:
        return None
    return "Enter a whole number greater than zero."

def ask_hhmm_range(end_example):
    """Prompts for a 5-minute-aligned start and end time (HHMM, UTC)."""
    start_hhmm = ui.text("Enter start time", hint="HHMM UTC, e.g. 0000", validate=validate_five_minute)
    ui.answer("Start", f"{start_hhmm} UTC")

    def validate_end(value):
        err = validate_five_minute(value)
        if err:
            return err
        if value < start_hhmm:
            return "Start time must be before or equal to end time."
        return None

    end_hhmm = ui.text("Enter end time", hint=f"HHMM UTC, e.g. {end_example}", validate=validate_end)
    ui.drop("Start")
    ui.answer("Time", f"{start_hhmm} → {end_hhmm} UTC, every 5 min")
    return start_hhmm, end_hhmm

def choose_existing_file():
    """Offers the feed files in the working directory (newest first), or a manually entered path."""
    candidates = [f for f in os.listdir('.') if os.path.isfile(f) and f.lower().endswith(('.json', '.json.gz', '.mmdb'))]
    candidates.sort(key=os.path.getmtime, reverse=True)
    if candidates:
        labels = [f"{name}  ({_fmt_bytes(os.path.getsize(name))})" for name in candidates]
        choice = ui.select("Select a feed file", labels + ["Enter a path manually"], subtitle=f"Feed files in {os.getcwd()}", sep_before=len(labels))
        if choice < len(candidates):
            return candidates[choice]
    return os.path.expanduser(ui.text("Enter the full path to your Spur Feed file"))

def finish(*messages):
    for message in messages:
        ui.ok(message)
    ui.out()
    ui.info("Script finished.")

def main():
    global API_TOKEN
    script_start_time = time.time()
    ui.enable()

    if os.environ.get('TOKEN') is None:
        user_token = ui.text("Enter your Spur API token", subtitle="No TOKEN environment variable found.")
        os.environ['TOKEN'] = user_token
        API_TOKEN = user_token

    current_date_ymd = datetime.date.today().strftime("%Y%m%d")
    current_time_hms = None
    decompressed_source_file_path = None
    base_feed_name = "UnknownFeed"
    is_feed_json = True
    multi_download_realtime_range = False
    realtime_range_start = None
    realtime_range_end = None

    if ui.confirm("Use an existing Spur Feed file?", default=False):
        provided_file_path = choose_existing_file()
        if not os.path.exists(provided_file_path):
            ui.error(f"Provided input file '{provided_file_path}' not found. Exiting.")
            sys.exit(1)

        decompressed_source_file_path = provided_file_path

        match = re.search(r'(\d{8})(\d{6})?(AnonRes|AnonResRT|Anonymous|IPGeoMMDB|IPGeoJSON|ServiceMetrics|DCH|AnonymousIPv6|AnonymousResidentialIPv6|AnonymousResidential|AnonymousResidentialRT|IPSummary|SimilarIPs|AIData)\.(json|mmdb|json\.gz)$', os.path.basename(provided_file_path), re.IGNORECASE)
        if match:
            current_date_ymd = match.group(1)
            if match.group(2):
                current_time_hms = match.group(2)
            base_feed_name = match.group(3)
            if base_feed_name in ["IPGeoMMDB", "Anonymous", "AnonymousResidential"] and provided_file_path.lower().endswith('.mmdb'):
                is_feed_json = False
        else:
            name_without_ext = os.path.splitext(os.path.basename(provided_file_path))[0]
            base_feed_name_candidate = re.sub(r'^\d{8}(\d{6})?', '', name_without_ext)
            if base_feed_name_candidate:
                base_feed_name = base_feed_name_candidate
                if "AnonRes" in base_feed_name and "AnonResRT" not in base_feed_name:
                    base_feed_name = base_feed_name.replace("AnonRes", "AnonymousResidential")
            else:
                base_feed_name = "CustomFeed"
            ui.note(f"Could not extract standard FeedName. Using derived name '{base_feed_name}'.", 'warn')
            if not provided_file_path.lower().endswith('.json') and not provided_file_path.lower().endswith('.json.gz'):
                is_feed_json = False

        if not is_feed_json:
            ui.note("Non-JSON file detected. Filtering will proceed and output in JSONL format.")

        ui.answer("File", os.path.basename(provided_file_path))
        ui.set_done(2)

    else:
        selected_feed = None
        category_keys = list(MENU_STRUCTURE.keys())
        category_names = [MENU_STRUCTURE[k]['category'] for k in category_keys]

        while selected_feed is None:
            category_choice = ui.select("Select a feed category", category_names + ["Exit"], sep_before=len(category_names), exit_keys=('q',))
            if category_choice == 'q' or category_choice == len(category_names):
                ui.info("Exiting...")
                sys.exit(0)

            active_category = MENU_STRUCTURE[category_keys[category_choice]]
            option_items = list(active_category['options'].values())
            ui.answer("Category", active_category['category'])

            feed_choice = ui.select("Select a feed", [o['name'] for o in option_items] + ["Go back"], sep_before=len(option_items), exit_keys=('esc',))
            if feed_choice == 'esc' or feed_choice == len(option_items):
                ui.drop("Category")
                continue
            selected_feed = option_items[feed_choice]

        ui.answer("Feed", selected_feed['name'])

        # Map the selected dictionary item to the expected variables
        api_url = selected_feed.get("url")
        base_feed_name = selected_feed["base_feed_name"]
        needs_decompression = selected_feed["needs_decompression"]
        output_ext = selected_feed["output_ext"]
        is_historical = selected_feed["is_historical"]
        is_feed_json = selected_feed["is_json"]

        ui.set_done(1)

        if is_historical:
            date_mode = ui.select("Download a single date or a date range?", ["Single date", "Date range"])

            if date_mode == 1:
                # --- Multi-date historical download (bypasses filtering, mirrors the RT time-range behavior) ---
                ui.answer("Mode", "Date range")
                date_range_input = ui.text("Enter date range", hint="YYYYMMDD-YYYYMMDD", validate=validate_date_range)
                range_start_ymd, range_end_ymd = date_range_input.split('-')
                date_list = generate_date_range(range_start_ymd, range_end_ymd)
                ui.drop("Mode")
                ui.answer("Dates", f"{range_start_ymd} → {range_end_ymd} ({len(date_list)} days)")

                token = os.environ.get('TOKEN')
                all_saved_paths = []
                all_failed = []

                if base_feed_name == "AnonymousResidentialRT":
                    per_day_range = ui.confirm("Download a time range for each date?", subtitle="Realtime feeds are published every 5 minutes.", default=True)

                    if per_day_range:
                        start_hhmm, end_hhmm = ask_hhmm_range("2355")
                        hhmm_list = generate_hhmm_range(start_hhmm, end_hhmm)
                        ui.set_done(2)
                        ui.screen()
                        ui.heading(f"Downloading {len(date_list) * len(hhmm_list)} files")
                        for date_ymd in date_list:
                            saved_paths, failed_times = download_realtime_range_files(
                                selected_feed["url_template"], date_ymd, hhmm_list, base_feed_name, token
                            )
                            all_saved_paths.extend(saved_paths)
                            all_failed.extend(f"{date_ymd} {t}" for t in failed_times)
                    else:
                        historical_time_hhmm = ui.text("Enter time to fetch for each date", hint="HHMM UTC, e.g. 0000", validate=validate_hhmm)
                        ui.answer("Time", f"{historical_time_hhmm} UTC")
                        ui.set_done(2)
                        ui.screen()
                        ui.heading(f"Downloading {len(date_list)} files")
                        for idx, date_ymd in enumerate(date_list, start=1):
                            url = selected_feed["url_template"].format(date_ymd, historical_time_hhmm)
                            output_path = f"{date_ymd}{historical_time_hhmm}00{base_feed_name}.json.gz"
                            if download_with_resume(url, token, output_path, label=f"[{idx}/{len(date_list)}] {output_path}"):
                                all_saved_paths.append(output_path)
                            else:
                                all_failed.append(f"{date_ymd} {historical_time_hhmm}")
                else:
                    ui.set_done(2)
                    ui.screen()
                    ui.heading(f"Downloading {len(date_list)} files")
                    all_saved_paths, all_failed = download_historical_date_range_files(
                        selected_feed["url_template"], date_list, base_feed_name, needs_decompression, output_ext, token
                    )

                ui.out()
                ui.set_done(3, idle=True)
                ui.refresh_rail()
                if all_saved_paths:
                    ui.ok(f"Downloaded {len(all_saved_paths)} file(s), kept separate and gzipped")
                else:
                    ui.error("Download failed. Exiting.")
                    sys.exit(1)

                if all_failed:
                    ui.warn(f"{len(all_failed)} item(s) failed and were skipped: {', '.join(all_failed)}")

                ui.out()
                ui.info("Script finished.")
                sys.exit(0)

            else:
                historical_date_ymd = ui.text("Enter date", hint="YYYYMMDD", validate=validate_ymd)
                ui.answer("Date", historical_date_ymd)

                if base_feed_name == "AnonymousResidentialRT":
                    per_day_range = ui.confirm("Download multiple Realtime feeds across a time range?", subtitle="Realtime feeds are published every 5 minutes.", default=True)

                    if per_day_range:
                        start_hhmm, end_hhmm = ask_hhmm_range("0300")
                        realtime_range_start = start_hhmm
                        realtime_range_end = end_hhmm
                        multi_download_realtime_range = True
                        current_time_hms = f"{start_hhmm}-{end_hhmm}"
                    else:
                        historical_time_hhmm = ui.text("Enter time", hint="HHMM UTC, e.g. 0000", validate=validate_hhmm)
                        ui.answer("Time", f"{historical_time_hhmm} UTC")
                        api_url = selected_feed["url_template"].format(historical_date_ymd, historical_time_hhmm)
                        current_time_hms = historical_time_hhmm + '00'
                else:
                    api_url = selected_feed["url_template"].format(historical_date_ymd)

                current_date_ymd = historical_date_ymd

        ui.set_done(2)

        download_filename_temp = f"{current_date_ymd}"

        temp_base_feed_name = base_feed_name
        if base_feed_name in ["AnonResRT", "AnonymousResidentialRT"]:
            if not current_time_hms:
                current_time_hms = datetime.datetime.now().strftime("%H%M%S")
            download_filename_temp += f"{current_time_hms}"

        if base_feed_name == "AnonymousResidentialHist": temp_base_feed_name = "AnonymousResidential"
        elif base_feed_name == "AnonymousHist": temp_base_feed_name = "Anonymous"

        download_filename_temp += temp_base_feed_name

        if multi_download_realtime_range:
            hhmm_list = generate_hhmm_range(realtime_range_start, realtime_range_end)
            ui.screen()
            ui.heading(f"Downloading {len(hhmm_list)} Realtime time slices")
            saved_paths, failed_times = download_realtime_range_files(
                selected_feed["url_template"], current_date_ymd, hhmm_list, base_feed_name, os.environ.get('TOKEN')
            )

            ui.out()
            ui.set_done(3, idle=True)
            ui.refresh_rail()
            if saved_paths:
                ui.ok(f"Downloaded {len(saved_paths)}/{len(hhmm_list)} time slices, kept separate and gzipped")
            else:
                ui.error("Download failed. Exiting.")
                sys.exit(1)

            ui.out()
            ui.info("Script finished.")
            sys.exit(0)

        # Unified download logic: just download the file, don't decompress yet.
        if needs_decompression:
            download_filename_temp += ".json.gz"
        else:
            download_filename_temp += output_ext

        ui.screen()
        ui.heading("Downloading")
        source_file_path = download_raw_file_to_disk(api_url, os.environ.get('TOKEN'), download_filename_temp)

        if not source_file_path:
            ui.error("Download failed. Exiting.")
            sys.exit(1)

        decompressed_source_file_path = source_file_path
        ui.answer("File", f"{os.path.basename(source_file_path)}  {_fmt_bytes(os.path.getsize(source_file_path))}")

        if not is_feed_json:
            ui.note("This is not a JSON file, but it can be filtered.")

    if not decompressed_source_file_path or not os.path.exists(decompressed_source_file_path):
        ui.error(f"Critical Error: File '{decompressed_source_file_path}' not found. Exiting.")
        sys.exit(1)

    if decompressed_source_file_path.lower().endswith('.gz'):
        ui.set_done(2)
        if not ui.confirm("Uncompress the feed?", subtitle=os.path.basename(decompressed_source_file_path), default=True):
            ui.set_done(3, idle=True)
            ui.screen()
            finish(f"File available at: {decompressed_source_file_path}")
            sys.exit(0)

        ui.screen()
        ui.heading("Uncompressing")
        decompressed_path = decompress_file(decompressed_source_file_path)
        if not decompressed_path:
            ui.error("Decompression failed. Exiting.")
            sys.exit(1)
        decompressed_source_file_path = decompressed_path
        ui.answer("File", f"{os.path.basename(decompressed_path)}  {_fmt_bytes(os.path.getsize(decompressed_path))}")

    ui.set_done(3)
    filter_criteria = []

    if ui.confirm("Do you want to filter the data?", default=True):
        while True:
            # Allow MMDB files to be filtered in addition to JSON
            if not decompressed_source_file_path.lower().endswith(('.json', '.mmdb')):
                ui.error(f"The file '{os.path.basename(decompressed_source_file_path)}' is not a supported format for filtering (.json or .mmdb).")
                sys.exit(1)

            current_filter_key = None
            current_keywords = []
            current_match_type_keywords = 'AND'

            if ui.confirm("Filter by a specific key?", default=True):
                key_sample_size = int(ui.text("How many records to sample for keys?", hint="⏎ for 500,000", default="500000", validate=validate_count))

                ui.screen()
                ui.heading(f"Analyzing the first {key_sample_size:,} records for filterable keys")

                suggested_keys = set()
                flattened_keys = set()
                seen, last_update = 0, 0
                for record in record_iterator(decompressed_source_file_path, is_feed_json, key_sample_size):
                    seen += 1
                    temp_flattened = flatten_json(record)
                    flattened_keys.update(temp_flattened.keys())
                    for key in temp_flattened.keys():
                        match = re.match(r'(.+?)_\d+_(.+)', key)
                        if match:
                            suggested_keys.add(f"{match.group(1)}_{match.group(2)}")
                        else:
                            suggested_keys.add(key)
                    now = time.time()
                    if now - last_update >= 0.2:
                        ui.progress(seen / key_sample_size * 100, f"{seen:,} records · {len(suggested_keys):,} keys")
                        last_update = now
                ui.end_progress()

                if suggested_keys:
                    current_filter_key = ui.search_select(
                        "Pick a key to filter on", sorted(suggested_keys),
                        subtitle=f"{len(suggested_keys):,} keys found in the first {seen:,} records"
                    )
                else:
                    current_filter_key = ui.text("Enter the exact key name for this filter")
                if not current_filter_key: continue
                ui.answer("Key", current_filter_key)

                if ui.confirm(f"See sample values for '{current_filter_key}'?", default=False):
                    val_sample_size = int(ui.text("How many records to sample for values?", hint="⏎ for 500,000", default="500000", validate=validate_count))

                    ui.screen()
                    ui.heading(f"Analyzing the first {val_sample_size:,} records for values")

                    unique_values = set()
                    target_flattened_keys = []
                    match = re.match(r'(.+?)_(.+)', current_filter_key)
                    if match:
                        root_key, sub_key = match.groups()
                        for f_key in flattened_keys:
                            if f_key.startswith(f"{root_key}_") and f_key.endswith(f"_{sub_key}"):
                                target_flattened_keys.append(f_key)
                    else:
                        if current_filter_key in flattened_keys:
                            target_flattened_keys.append(current_filter_key)

                    if target_flattened_keys:
                        seen, last_update = 0, 0
                        for record in record_iterator(decompressed_source_file_path, is_feed_json, val_sample_size):
                            seen += 1
                            flattened_obj = flatten_json(record)
                            for f_key in target_flattened_keys:
                                value = flattened_obj.get(f_key)
                                if value is not None:
                                    if isinstance(value, str):
                                        for individual_value in value.split(','):
                                            unique_values.add(individual_value.strip())
                                    else:
                                        unique_values.add(str(value).strip())
                            now = time.time()
                            if now - last_update >= 0.2:
                                ui.progress(seen / val_sample_size * 100, f"{seen:,} records · {len(unique_values):,} unique values")
                                last_update = now
                        ui.end_progress()

                    ui.screen()
                    if unique_values:
                        ui.out(ui.dim(f"  Unique values found ({len(unique_values):,})"))
                        for line in ui.columns(sorted(unique_values)):
                            ui.out(line)
                    else:
                        ui.out(ui.dim("  No values found in sample."))
                    ui.out()

                    if not ui.confirm("Proceed filtering for this key?", default=True, clear=False):
                        ui.drop("Key")
                        continue

                current_keywords_input = ui.text(f"Enter keywords for '{current_filter_key}'", body=keyword_tips_lines())

            else:
                current_keywords_input = ui.text("Enter keywords for general search", body=keyword_tips_lines())

            if current_keywords_input:
                current_keywords = [kw.strip().lower() for kw in current_keywords_input.split(',') if kw.strip()]

                if current_keywords:
                    if len(current_keywords) > 1:
                        match_type_choice = ui.select("Match all keywords or any keyword?", ["Match all keywords (AND)", "Match any keyword (OR)"])
                        current_match_type_keywords = 'AND' if match_type_choice == 0 else 'OR'

                    filter_criteria.append({
                        'key': current_filter_key,
                        'keywords': current_keywords,
                        'match_type_keywords': current_match_type_keywords
                    })
                    ui.answer(f"Filter {len(filter_criteria)}", f"{current_filter_key or 'general search'}: {', '.join(current_keywords)}")

            ui.drop("Key")
            if not ui.confirm("Add another filter condition?", default=False):
                break

    overall_match_type = 'AND'
    if len(filter_criteria) > 1:
        overall_choice = ui.select("Apply all filter conditions or any condition?", ["Match all conditions (AND)", "Match any condition (OR)"])
        overall_match_type = 'AND' if overall_choice == 0 else 'OR'

    if not filter_criteria:
        # --- NO FILTERING: BYPASS PROCESSING ---
        current_filename = os.path.basename(decompressed_source_file_path)
        user_output_filename = ui.text("Output filename", default=current_filename, hint="⏎ keeps the current name")

        if user_output_filename == current_filename:
            result_message = f"File available at: {decompressed_source_file_path}"
        else:
            # Rename/Move source to user specific name
            final_output_path = get_output_filename(
                current_date_ymd, current_time_hms, base_feed_name, user_output_filename, [], overall_match_type
            )
            # Ensure we don't overwrite if they just typed the same name
            if os.path.abspath(final_output_path) != os.path.abspath(decompressed_source_file_path):
                shutil.move(decompressed_source_file_path, final_output_path)
                result_message = f"File moved to: {final_output_path}"
            else:
                result_message = f"File available at: {final_output_path}"

        ui.set_done(3, idle=True)
        ui.screen()
        finish(result_message)
        sys.exit(0)

    # Generate the dynamic default filename based on selected filters
    default_generated_filename = get_output_filename(
        current_date_ymd,
        current_time_hms,
        base_feed_name,
        "",
        filter_criteria,
        overall_match_type
    )

    user_output_filename = ui.text("Output filename", default=default_generated_filename, hint="⏎ for the default")

    filtered_output_filename = get_output_filename(
        current_date_ymd,
        current_time_hms,
        base_feed_name,
        user_output_filename,
        filter_criteria,
        overall_match_type
    )
    output_file_path = os.path.join(os.getcwd(), filtered_output_filename)

    # --- CRITICAL SAFEGUARD: Prevent overwriting source file ---
    if os.path.abspath(output_file_path) == os.path.abspath(decompressed_source_file_path):
        ui.note("Output filename matches source filename. Prepending 'Filtered_' to prevent data loss.", 'warn')
        dirname, basename = os.path.split(output_file_path)
        output_file_path = os.path.join(dirname, "Filtered_" + basename)

    ui.answer("Output", os.path.basename(output_file_path))
    ui.screen()
    ui.heading("Filtering")

    start_time = time.time()

    if is_feed_json:
        records_exported_count = 0
        total_records_parsed = 0
        chunks_completed = 0
        last_ui_update_time = start_time

        NUM_PARALLEL_PROCESSORS = os.cpu_count() if os.cpu_count() else 4

        try:
            with open(output_file_path, 'w', encoding='utf-8') as outfile:
                chunks = get_file_chunks(decompressed_source_file_path, NUM_PARALLEL_PROCESSORS)
                total_chunks = len(chunks)

                ui.info(f"Using {NUM_PARALLEL_PROCESSORS} parallel processors across {total_chunks} data chunks")

                with multiprocessing.Pool(processes=NUM_PARALLEL_PROCESSORS) as pool:
                    results_iterator = pool.imap_unordered(
                        process_file_chunk,
                        [(decompressed_source_file_path, start, end, filter_criteria, overall_match_type) for start, end in chunks]
                    )

                    for matching_lines_in_chunk, lines_parsed_in_chunk in results_iterator:
                        chunks_completed += 1
                        total_records_parsed += lines_parsed_in_chunk
                        try:
                            for line in matching_lines_in_chunk:
                                outfile.write(line + '\n')
                                records_exported_count += 1

                            current_time = time.time()
                            if current_time - last_ui_update_time >= 0.5 or chunks_completed == total_chunks:
                                elapsed_time = current_time - start_time
                                records_per_second = total_records_parsed / elapsed_time if elapsed_time > 0 else 0
                                progress_pct = (chunks_completed / total_chunks) * 100
                                ui.progress(progress_pct, f"{chunks_completed}/{total_chunks} chunks · {records_exported_count:,} exported · {records_per_second:,.0f} parsed/s")
                                last_ui_update_time = current_time
                        except Exception as exc:
                            ui.end_progress()
                            ui.error(f"Error processing chunk: {exc}")

                ui.end_progress()
                ui.ok(f"Exported {records_exported_count:,} records to {os.path.basename(output_file_path)}")
                ui.info(f"Saved in {ui.tilde(os.path.dirname(output_file_path))}")

        except Exception as e:
            ui.end_progress()
            ui.error(f"Error during export: {e}")
            sys.exit(1)
    else:  # It's an MMDB file
        records_exported_count, total_records_parsed = process_mmdb_and_filter(
            decompressed_source_file_path,
            output_file_path,
            filter_criteria,
            overall_match_type
        )
        if total_records_parsed > 0:
            ui.ok(f"Exported {records_exported_count:,} records to {os.path.basename(output_file_path)}")
            ui.info(f"Saved in {ui.tilde(os.path.dirname(output_file_path))}")
        else:
            ui.error("Processing finished, but no records were exported.")

    total_elapsed_seconds = time.time() - script_start_time
    ui.set_done(4)
    ui.refresh_rail()
    ui.out()
    ui.info(f"Script finished in {int(total_elapsed_seconds // 60)} min {int(total_elapsed_seconds % 60)} sec.")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        ui.end_progress()
        print("\nCancelled.", file=sys.stderr)
        sys.exit(130)
