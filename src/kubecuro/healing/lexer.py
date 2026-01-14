"""
KUBECURO LEXER - Phase 1.1 (The Refurbisher)
--------------------------------------------
PURPOSE: 
Fixes character-level syntax errors in "Dead" YAML before the structural parser 
(ruamel.yaml) takes over. This ensures the parser doesn't crash on simple 
formatting typos.

SOLVED CASES (15):
1.  TAB REMOVAL: Converts illegal '\t' characters to 2 spaces.
2.  STUCK COLONS: Fixes 'kind:Pod' -> 'kind: Pod'.
3.  STUCK DASHES: Fixes '-image: nginx' -> '- image: nginx'.
4.  TRAILING WHITESPACE: Cleans ghost spaces while preserving logic.
5.  ESCAPED QUOTES: Properly handles \" inside double-quoted strings.
6.  NESTED QUOTES: Recognizes single quotes inside double quotes and vice versa.
7.  QUOTED COLONS: Prevents splitting on colons inside "https://..." URLs.
8.  INLINE COMMENTS: Normalizes spacing between values and '#' comments.
9.  QUOTED HASHMARKS: Protects "Tag #42" from being treated as a comment.
10. SPACE-AWARE COMMENTS: Only treats '#' as a comment if preceded by a space.
11. PURE COMMENT CLEANING: Trims trailing spaces on lines that are only comments.
12. BLOCK SCALAR PROTECTION: Detects '|' and '>' and stops modification of inner text.
13. BLOCK AUTO-EXIT: Calculates indents to know when a sacred script block ends.
14. BACKSLASH WRAPPING: Detects '\' continuation and protects the next line's indent.
15. EMPTY KEY HANDLING: Safely handles keys with comments but no values.
"""

import re

class RawLexer:
    def __init__(self):
        # FIX: Updated Group 1 (\s*-?\s*) to actually capture the dash if it exists
        self.kv_pattern = re.compile(r'^(\s*-?\s*)([^#:"\']+)\s*:\s*(.*)$')
        self.in_block = False
        self.block_indent = 0
        self.skip_next = False

    def is_likely_new_key(self, line: str) -> bool:
        return bool(self.kv_pattern.match(line))

    def _find_comment_split(self, text: str) -> int:
        in_double_quote = False
        in_single_quote = False
        escaped = False
        for i, char in enumerate(text):
            if escaped:
                escaped = False
                continue
            if char == '\\':
                escaped = True
                continue
            if char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            if char == '#' and not in_double_quote and not in_single_quote:
                if i == 0 or text[i-1].isspace():
                    return i
        return -1

    def repair_line(self, line: str) -> str:
        line = line.replace('\t', '  ')
        if not line.strip(): return line

        if self.skip_next:
            processed_line = line.rstrip()
            self.skip_next = processed_line.endswith('\\')
            return processed_line

        current_content = line.lstrip()
        current_indent = len(line) - len(current_content)
        
        if self.in_block:
            if current_indent <= self.block_indent and self.is_likely_new_key(line):
                self.in_block = False 
            else:
                return line

        if current_content.startswith('#'):
            return line.rstrip()

        match = self.kv_pattern.match(line)
        if match:
            prefix, key, remainder = match.groups()
            
            # --- INTELLIGENT SPACING (The fix we discussed) ---
            if prefix.endswith('-'):
                cleaned = f"{prefix} {key.strip()}:"
            else:
                cleaned = f"{prefix}{key.strip()}:"

            # Split value/comment
            split_idx = self._find_comment_split(remainder)
            if split_idx != -1:
                val_part = remainder[:split_idx].rstrip()
                comment_part = remainder[split_idx:].strip()
            else:
                val_part = remainder.strip()
                comment_part = ""

            # State Triggers
            if val_part.endswith('\\'): self.skip_next = True
            if val_part in ['|', '|-', '>', '>-']:
                self.in_block = True
                self.block_indent = current_indent

            # --- RECONSTRUCTION (Append values and comments) ---
            if val_part: 
                cleaned += f" {val_part}"
            if comment_part: 
                cleaned += f"  {comment_part}"
            return cleaned

        return line.rstrip()

    def process_string(self, raw_yaml: str) -> str:
        self.in_block = False
        self.skip_next = False
        return "\n".join([self.repair_line(l) for l in raw_yaml.splitlines()])
