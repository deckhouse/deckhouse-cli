# htpasswd

A self-contained, pure-Go analog of Apache `htpasswd`. It manages password files and hashes passwords without requiring the external `htpasswd` binary (from `apache2-utils` / `httpd-tools`). It is a drop-in for the common `htpasswd` invocations and interoperates with real htpasswd, nginx, Apache, and Dex in both directions.

## Usage

```
d8 tools htpasswd [-cbdps... -C cost -r rounds] passwordfile username
d8 tools htpasswd -b [...] passwordfile username password
d8 tools htpasswd -n [-bmBdps...] [username]
d8 tools htpasswd -D passwordfile username
d8 tools htpasswd -v passwordfile username
```

## Flags

Every Apache htpasswd flag is supported, including flag bundling (`-nbB`). Three flags are **d8 extensions** with no Apache htpasswd equivalent: the SHA-crypt algorithm flags `-2` (SHA-256) and `-5` (SHA-512), and `-r` (rounds).

| Flag | Meaning |
|------|---------|
| `-c` | Create a new password file, overwriting any existing one. |
| `-n` | Do not update a file; print the result to stdout. |
| `-D` | Delete the given user from the password file. |
| `-v` | Verify the given password for the user. |
| `-b` | Batch mode: take the password from the command line. |
| `-i` | Read the password from stdin without confirmation. |
| `-C` | bcrypt cost/work factor (4–31); only with `-B`. |
| `-r` | SHA-256/512 rounds (1000–999999999); only with `-2`/`-5`. **d8 extension.** |

## Algorithms

Select one; the default is bcrypt.

| Flag | Scheme | Notes |
|------|--------|-------|
| `-B` | bcrypt (`$2y$`) | Secure. The default. |
| `-m` | Apache MD5 / apr1 (`$apr1$`) | Legacy htpasswd default. |
| `-2` | SHA-256 crypt (`$5$`) | Secure. **d8 extension** (not in Apache htpasswd). |
| `-5` | SHA-512 crypt (`$6$`) | Secure. **d8 extension** (not in Apache htpasswd). |
| `-d` | CRYPT / DES | **Insecure**: only the first 8 characters are used. |
| `-s` | SHA-1 (`{SHA}`) | **Insecure**: unsalted. |
| `-p` | plaintext | **Insecure**: no hashing. |

## Differences from Apache htpasswd

Apache htpasswd defaults to apr1-MD5 (and, for `-B`, to bcrypt cost 5). `d8 tools htpasswd` defaults to **bcrypt at cost 10** so the output is strong and directly usable by `d8 iam user create` / `d8 iam user reset-password`. d8 also adds extensions Apache htpasswd lacks: with `-n` and no username it prints the bare hash (Apache htpasswd always requires a username and prints `username:hash`), which is exactly what `--password-hash` expects; and the SHA-crypt algorithms `-2`/`-5` plus the `-r` rounds flag (Apache htpasswd has no `-2`, `-5`, or `-r`). Each algorithm flag Apache htpasswd also defines (`-B`, `-m`, `-d`, `-s`, `-p`) behaves identically, and bcrypt output uses the `$2y$` identifier for byte-level parity. One further divergence: d8 allows bcrypt `-C` up to 31 (Apache caps it at 17). Otherwise d8 mirrors Apache htpasswd's exit codes — 2 (usage/syntax), 3 (verification failure), 5 (over-long username), 6 (bad or missing user), and 1 (file-access errors).

Parity is verified against Apache httpd `htpasswd` 2.4.58 (`apache2-utils`); the crypt-family hash outputs are additionally cross-checked byte-for-byte against OpenSSL 3.0.13, libxcrypt 4.4.36, and Python 3.12.3 `crypt`.

## Examples

```bash
# Create a file and add a user (prompts for the password)
d8 tools htpasswd -c users.htpasswd alice

# Add/update a user non-interactively (password from stdin)
echo -n 'S3cret!' | d8 tools htpasswd -i users.htpasswd bob

# Verify a password, then delete the user
d8 tools htpasswd -bv users.htpasswd bob 'S3cret!'
d8 tools htpasswd -D users.htpasswd bob

# Print a bcrypt hash for 'd8 iam user ... --password-hash'
HASH="$(echo -n 'Test12345!' | d8 tools htpasswd -ni)"
d8 iam user reset-password test-user --password-hash "$HASH"

# apr1 line for an Ingress basic-auth secret; SHA-512 crypt with custom rounds
d8 tools htpasswd -nbm admin 'S3cret!'
d8 tools htpasswd -nb5 -r 100000 admin 'S3cret!'
```
